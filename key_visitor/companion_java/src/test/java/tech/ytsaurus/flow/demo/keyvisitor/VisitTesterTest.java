package tech.ytsaurus.flow.demo.keyvisitor;

import java.util.List;

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
import tech.ytsaurus.flow.row.Visit;
import tech.ytsaurus.flow.state.InternalState;
import tech.ytsaurus.flow.state.StatesHolder;
import tech.ytsaurus.flow.stream.FlowStreams;
import tech.ytsaurus.flow.stream.StreamIdsMapping;
import tech.ytsaurus.flow.stream.StreamSpecs;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.ysontree.YTree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Offline tests of the visit logic, mirroring the Go variant's {@code main_test.go}: the visit
 * choreography is driven through {@link Computation#doProcess} with hand-built requests — no
 * cluster needed. The engine-side subject (key tracking, the sweep, the final pass) is out of
 * scope here and is proven by the live run.
 */
public class VisitTesterTest {

    private static final TableSchema KEYS_SCHEMA = TableSchema.builder()
            .addValue("key", TiType.string())
            .addValue("payload", TiType.string())
            .build();

    private static final TableSchema VISITS_SCHEMA = TableSchema.builder()
            .addValue("key", TiType.string())
            .addValue("payload", TiType.string())
            .addValue("visit_index", TiType.int64())
            .build();

    private static final TableSchema KEY_SCHEMA = TableSchema.builder()
            .add(ColumnSchema.builder("hash", ColumnValueType.UINT64, true).build())
            .addValue("key", TiType.string())
            .build();

    private Computation computation;
    private StreamSpecs streamSpecs;
    private Job job;
    private java.util.Map<String, StatesHolder<InternalState>> states;

    @BeforeEach
    public void setUp() {
        computation = Computation.builder()
                .setComputationId("tester")
                .setProcessFunction(new VisitTester())
                .build();

        var mapping = StreamIdsMapping.builder()
                .addMapping("keys", 0L)
                .addMapping("visits", 1L)
                .build();
        streamSpecs = new StreamSpecs(mapping, List.of(
                FlowStreams.raw("keys", KEYS_SCHEMA),
                FlowStreams.raw("visits", VISITS_SCHEMA)));

        var staticSpec = YTree.mapBuilder()
                .key("parameters").beginMap()
                .key("internal_states").beginList().value("user_state").endList()
                .endMap()
                .endMap()
                .build();
        var dynamicSpec = YTree.mapBuilder().key("parameters").beginMap().endMap().endMap().build();

        job = new Job(GUID.create(), "tester", streamSpecs, staticSpec, dynamicSpec, KEY_SCHEMA);

        states = new java.util.HashMap<>();
        states.put("user_state", new StatesHolder<>("user_state", KEY_SCHEMA));
    }

    private Payload keyOf(String key) {
        return new Payload(
                new UnversionedRow(List.of(
                        new UnversionedValue(0, ColumnValueType.UINT64, false, 1L),
                        new UnversionedValue(1, ColumnValueType.STRING, false, key.getBytes()))),
                KEY_SCHEMA);
    }

    private ExtendedMessage messageOf(String key, String payload) {
        return ExtendedMessage.builder()
                .setMessageId(GUID.create().toString())
                .setStreamId("keys")
                .setKey(keyOf(key))
                .setPayload(new Payload(
                        new UnversionedRow(List.of(
                                new UnversionedValue(0, ColumnValueType.STRING, false, key.getBytes()),
                                new UnversionedValue(1, ColumnValueType.STRING, false, payload.getBytes()))),
                        KEYS_SCHEMA))
                .build();
    }

    private Visit visitOf(String key) {
        return new Visit(GUID.create().toString(), 0, 0, "visit_iter", keyOf(key));
    }

    private List<Message> process(List<ExtendedMessage> messages, List<Visit> visits) throws Exception {
        var response = computation.doProcess(RequestContext.builder()
                .setComputationId("tester")
                .setRequestId(GUID.create())
                .setJobId(job.getJobId())
                .setJob(job)
                .setMessages(messages)
                .setVisits(visits)
                .setInternalStates(states)
                .setStreamSpecsOverride(streamSpecs)
                .build());
        return response.getTransformResults().stream()
                .flatMap(result -> result.getMessages().stream())
                .toList();
    }

    @Test
    public void messageStoresStateAndEmitsNothing() throws Exception {
        var output = process(List.of(messageOf("k1", "v1")), List.of());
        assertTrue(output.isEmpty());
    }

    @Test
    public void visitEmitsStoredPayloadWithCounter() throws Exception {
        process(List.of(messageOf("k1", "v1")), List.of());

        var output = process(List.of(), List.of(visitOf("k1")));
        assertEquals(1, output.size());
        assertEquals("k1", output.get(0).get("key", String.class));
        assertEquals("v1", output.get(0).get("payload", String.class));
        assertEquals(1L, output.get(0).get("visit_index", Long.class));

        output = process(List.of(), List.of(visitOf("k1")));
        assertEquals(1, output.size());
        assertEquals(2L, output.get(0).get("visit_index", Long.class));
    }

    @Test
    public void visitOfUnseededKeyEmitsNothing() throws Exception {
        process(List.of(messageOf("k1", "v1")), List.of());

        var output = process(List.of(), List.of(visitOf("other")));
        assertTrue(output.isEmpty());
    }

    @Test
    public void laterPayloadSupersedesAndCounterSurvives() throws Exception {
        process(List.of(messageOf("k1", "v1")), List.of());
        var first = process(List.of(), List.of(visitOf("k1")));
        assertEquals("v1", first.get(0).get("payload", String.class));

        process(List.of(messageOf("k1", "v2")), List.of());
        var second = process(List.of(), List.of(visitOf("k1")));
        assertEquals(1, second.size());
        assertEquals("v2", second.get(0).get("payload", String.class));
        assertEquals(2L, second.get(0).get("visit_index", Long.class));
    }
}
