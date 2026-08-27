package tech.ytsaurus.flow.demo.transformhighthroughput;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.job.Job;
import tech.ytsaurus.flow.request.RequestContext;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Message;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.codec.DefaultYsonCodec;
import tech.ytsaurus.flow.row.codec.YsonByteArrayCodec;
import tech.ytsaurus.flow.state.InternalState;
import tech.ytsaurus.flow.state.StatesHolder;
import tech.ytsaurus.flow.stream.FlowStreams;
import tech.ytsaurus.flow.stream.StreamIdsMapping;
import tech.ytsaurus.flow.stream.StreamSpecs;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.ysontree.YTree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Offline proof of the reducer logic, mirroring the Go variant's {@code main_test.go}: the
 * per-key counting, the re-emit, the accumulation over prior state, the first-batch shape (no
 * state entry yet) and the deterministic first-appearance grouping order are pinned here by
 * driving {@link Computation#doProcess} with hand-built requests — no cluster needed. The
 * engine-side subject (epoch commit, the states table, the queue sink) is out of scope and is
 * proven by the live run.
 */
public class ReducerTest {

    private static final TableSchema EVENT_SCHEMA = TableSchema.builder()
            .addValue("key", TiType.string())
            .addValue("data", TiType.string())
            .build();

    private static final TableSchema KEY_SCHEMA = TableSchema.builder()
            .add(ColumnSchema.builder("hash", ColumnValueType.UINT64, true).build())
            .addValue("key", TiType.string())
            .build();

    private static final YsonByteArrayCodec<ReducerState> CODEC =
            new YsonByteArrayCodec<>(ReducerState.class, DefaultYsonCodec.INSTANCE);

    private Computation computation;
    private StreamSpecs streamSpecs;
    private Job job;
    private Map<String, StatesHolder<InternalState>> states;

    @BeforeEach
    public void setUp() {
        computation = Computation.builder()
                .setComputationId("Reducer")
                .setProcessFunction(new Reducer())
                .build();

        var mapping = StreamIdsMapping.builder()
                .addMapping("event", 0L)
                .addMapping("out", 1L)
                .build();
        streamSpecs = new StreamSpecs(mapping, List.of(
                FlowStreams.raw("event", EVENT_SCHEMA),
                FlowStreams.raw("out", EVENT_SCHEMA)));

        var staticSpec = YTree.mapBuilder()
                .key("parameters").beginMap()
                .key("internal_states").beginList().value("state").endList()
                .endMap()
                .endMap()
                .build();
        var dynamicSpec = YTree.mapBuilder().key("parameters").beginMap().endMap().endMap().build();

        job = new Job(GUID.create(), "Reducer", streamSpecs, staticSpec, dynamicSpec, KEY_SCHEMA);

        states = new HashMap<>();
        states.put("state", new StatesHolder<>("state", KEY_SCHEMA));
    }

    /** Stands in for the live farm_hash: any deterministic hash works offline because the key
     * itself is part of the grouping key. */
    private UnversionedRow keyRowOf(String key) {
        return new UnversionedRow(List.of(
                new UnversionedValue(0, ColumnValueType.UINT64, false, (long) key.length()),
                new UnversionedValue(1, ColumnValueType.STRING, false, key.getBytes())));
    }

    private ExtendedMessage messageOf(String key, String data) {
        return ExtendedMessage.builder()
                .setMessageId(GUID.create().toString())
                .setStreamId("event")
                .setKey(new Payload(keyRowOf(key), KEY_SCHEMA))
                .setPayload(new Payload(
                        new UnversionedRow(List.of(
                                new UnversionedValue(0, ColumnValueType.STRING, false, key.getBytes()),
                                new UnversionedValue(1, ColumnValueType.STRING, false, data.getBytes()))),
                        EVENT_SCHEMA))
                .build();
    }

    private List<Message> process(List<ExtendedMessage> messages) throws Exception {
        var response = computation.doProcess(RequestContext.builder()
                .setComputationId("Reducer")
                .setRequestId(GUID.create())
                .setJobId(job.getJobId())
                .setJob(job)
                .setMessages(messages)
                .setInternalStates(states)
                .setStreamSpecsOverride(streamSpecs)
                .build());
        return response.getTransformResults().stream()
                .flatMap(result -> result.getMessages().stream())
                .toList();
    }

    private ReducerState stateOf(String key) {
        InternalState state = states.get("state").get(keyRowOf(key));
        assertNotNull(state, "no state entry for key " + key);
        return CODEC.decode(state.getValue());
    }

    @Test
    public void countsAndReemitsMixedKeyBatch() throws Exception {
        var output = process(List.of(
                messageOf("alpha", "a1"),
                messageOf("be", "b1"),
                messageOf("alpha", "a2")));

        // Output is grouped in first-appearance key order, input order within a key.
        assertEquals(3, output.size());
        assertEquals("alpha", output.get(0).get("key", String.class));
        assertEquals("a1", output.get(0).get("data", String.class));
        assertEquals("alpha", output.get(1).get("key", String.class));
        assertEquals("a2", output.get(1).get("data", String.class));
        assertEquals("be", output.get(2).get("key", String.class));
        assertEquals("b1", output.get(2).get("data", String.class));

        var alpha = stateOf("alpha");
        assertEquals(2L, alpha.getCount());
        assertEquals("a2", alpha.getLastData());
        var be = stateOf("be");
        assertEquals(1L, be.getCount());
        assertEquals("b1", be.getLastData());
    }

    @Test
    public void startsFromZeroWithoutState() throws Exception {
        process(List.of(messageOf("fresh", "d1")));

        var fresh = stateOf("fresh");
        assertEquals(1L, fresh.getCount());
        assertEquals("d1", fresh.getLastData());
    }

    @Test
    public void accumulatesOverPriorState() throws Exception {
        var prior = new ReducerState();
        prior.setCount(190);
        prior.setLastData("old");
        states.get("state").load(keyRowOf("seen"), new InternalState(CODEC.encode(prior)));

        process(List.of(messageOf("seen", "new1"), messageOf("seen", "new2")));

        var seen = stateOf("seen");
        assertEquals(192L, seen.getCount());
        assertEquals("new2", seen.getLastData());
    }

    @Test
    public void leavesForeignKeysUntouched() throws Exception {
        var foreign = new ReducerState();
        foreign.setCount(7);
        foreign.setLastData("keep");
        states.get("state").load(keyRowOf("foreign"), new InternalState(CODEC.encode(foreign)));

        process(List.of(messageOf("touched", "d")));

        var modified = states.get("state").getModifiedStates();
        assertEquals(1, modified.size());
        assertFalse(modified.containsKey(keyRowOf("foreign")));
    }
}
