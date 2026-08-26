package tech.ytsaurus.flow.demo.computationcycles;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.computation.SourceComputation;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Message;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.flow.testutils.TestComputationHarness;
import tech.ytsaurus.flow.testutils.TestDoProcessRequest;
import tech.ytsaurus.flow.testutils.TestDoProcessResponse;
import tech.ytsaurus.typeinfo.TiType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Offline tests of the cycle logic, mirroring the Go variant's {@code main_test.go}: all six
 * computations are driven through {@link TestComputationHarness} — no cluster needed. The
 * engine-side subject of the scenario (the cyclic stream topology, the buffers, the epoch
 * transaction) is proven by the live run; what is proven here is the routing rules of every
 * computation and the reducer's exact counting.
 */
public class ComputationCyclesTest {

    private static final TableSchema DATA_SCHEMA = TableSchema.builder()
            .addValue("data", TiType.string())
            .build();

    private static final TableSchema KEY_SCHEMA = TableSchema.builder()
            .addValue("hash", TiType.uint64())
            .addValue("data", TiType.string())
            .build();

    // The state table backing the external state "/state": keyed exactly by the reducer's
    // group_by_schema, with the count as the single value column.
    private static final TableSchema STATE_SCHEMA = TableSchema.builder()
            .addValue("hash", TiType.uint64())
            .addValue("data", TiType.string())
            .addValue("count", TiType.int64())
            .build();

    private TestComputationHarness harness;

    @BeforeEach
    public void setUp() {
        var context = new PipelineContext();
        context.registerComputation(SourceComputation.builder()
                .setComputationId("reader")
                .setProcessFunction(new ReadData())
                .build());
        for (String computationId : new String[]{
                "transform_a", "swift_map_a", "transform_b", "swift_map_b"}) {
            context.registerComputation(Computation.builder()
                    .setComputationId(computationId)
                    .setProcessFunction(new CyclePassthrough())
                    .build());
        }
        context.registerComputation(Computation.builder()
                .setComputationId("reducer")
                .setProcessFunction(new Reducer())
                .build());
        harness = TestComputationHarness.builder()
                .setPipelineContext(context)
                .setPipelineSpec(getClass().getClassLoader().getResourceAsStream("pipeline.yson"))
                .addExternalStateSchema("/state", STATE_SCHEMA)
                .build();
    }

    private static Payload keyOf(String data) {
        // Any deterministic per-value hash works offline; live, the engine computes farm_hash.
        return new PayloadBuilder(KEY_SCHEMA)
                .set("hash", Integer.toUnsignedLong(data.hashCode()))
                .set("data", data)
                .finish();
    }

    private static ExtendedMessage dataMessage(String streamId, String data) {
        return ExtendedMessage.builder()
                .setMessageId(GUID.create().toString())
                .setStreamId(streamId)
                .setKey(keyOf(data))
                .setPayload(new PayloadBuilder(DATA_SCHEMA).set("data", data).finish())
                .build();
    }

    private Long countOf(TestDoProcessResponse response, String data) {
        return response.allStates().get(Reducer.COUNT_STATE, keyOf(data)).get()
                .map(payload -> payload.get("count", Long.class))
                .orElse(null);
    }

    /** Drives one computation over one input batch and re-wraps the outputs as inputs. */
    private List<ExtendedMessage> step(String computationId, List<ExtendedMessage> messages) {
        var response = harness.doProcess(TestDoProcessRequest.builder(computationId)
                .setMessages(messages)
                .build());
        var outputs = new ArrayList<ExtendedMessage>();
        for (Message message : response.getOutputMessagesFlatten()) {
            outputs.add(dataMessage(message.getStreamId(), message.get("data", String.class)));
        }
        return outputs;
    }

    @Test
    public void testReaderRepublishesData() {
        var outputs = step("reader", List.of(dataMessage("input", "payload")));

        assertEquals(1, outputs.size());
        assertEquals("reader_output", outputs.get(0).getStreamId());
        assertEquals("payload", outputs.get(0).get("data", String.class));
    }

    @Test
    public void testTransformARoutesByInputStream() {
        // The heart of the cycle: a message fresh from the reader goes once around the loop
        // (→ ta1), a message coming back on sb1 is released to the reducer (→ ta2).
        var outputs = step("transform_a", List.of(
                dataMessage("reader_output", "payload"),
                dataMessage("sb1", "payload")));

        assertEquals(2, outputs.size());
        assertEquals("ta1", outputs.get(0).getStreamId());
        assertEquals("ta2", outputs.get(1).getStreamId());
        assertTrue(outputs.stream()
                .allMatch(message -> message.get("data", String.class).equals("payload")));
    }

    @Test
    public void testLoopSegmentsForward() {
        assertEquals("sa1", step("swift_map_a",
                List.of(dataMessage("ta1", "payload"))).get(0).getStreamId());
        assertEquals("tb1", step("transform_b",
                List.of(dataMessage("sa1", "payload"))).get(0).getStreamId());
        assertEquals("sb1", step("swift_map_b",
                List.of(dataMessage("tb1", "payload"))).get(0).getStreamId());
    }

    @Test
    public void testMissingPassthroughRuleIsAnError() {
        // ta2 is not an input stream of transform_a; a message with nowhere to go must be
        // reported rather than dropped.
        assertThrows(RuntimeException.class, () ->
                harness.doProcess(TestDoProcessRequest.builder("transform_a")
                        .setMessages(List.of(dataMessage("ta2", "payload")))
                        .build()));
    }

    @Test
    public void testReducerCountsFreshKey() {
        var response = harness.doProcess(TestDoProcessRequest.builder("reducer")
                .setMessages(List.of(
                        dataMessage("ta2", "payload"),
                        dataMessage("ta2", "payload"),
                        dataMessage("ta2", "payload")))
                .build());

        assertTrue(response.getOutputMessagesFlatten().isEmpty());
        assertEquals(3L, countOf(response, "payload"));
    }

    @Test
    public void testReducerAddsToPriorState() {
        var response = harness.doProcess(TestDoProcessRequest.builder("reducer")
                .setMessages(List.of(dataMessage("ta2", "payload"), dataMessage("ta2", "payload")))
                .setState(Reducer.COUNT_STATE, keyOf("payload"),
                        new PayloadBuilder(STATE_SCHEMA)
                                .set("hash", Integer.toUnsignedLong("payload".hashCode()))
                                .set("data", "payload")
                                .set("count", 5L)
                                .finish())
                .build());

        assertEquals(7L, countOf(response, "payload"));
    }

    @Test
    public void testReducerToleratesNullCount() {
        // Live, a key with no count yet can arrive as a present state row with the key columns
        // set and "count" null (see the README); the reducer must treat it as zero rather
        // than fail.
        var response = harness.doProcess(TestDoProcessRequest.builder("reducer")
                .setMessages(List.of(dataMessage("ta2", "payload")))
                .setState(Reducer.COUNT_STATE, keyOf("payload"),
                        new PayloadBuilder(STATE_SCHEMA)
                                .set("hash", Integer.toUnsignedLong("payload".hashCode()))
                                .set("data", "payload")
                                .finish())
                .build());

        assertEquals(1L, countOf(response, "payload"));
    }

    @Test
    public void testReducerGroupsMixedKeysItself() {
        var response = harness.doProcess(TestDoProcessRequest.builder("reducer")
                .setMessages(List.of(
                        dataMessage("ta2", "payload"),
                        dataMessage("ta2", "other"),
                        dataMessage("ta2", "payload")))
                .build());

        assertEquals(2L, countOf(response, "payload"));
        assertEquals(1L, countOf(response, "other"));
    }

    @Test
    public void testFullCycleSimulation() {
        // The scenario's 1000 identical rows, driven around the loop in batches of 30 — the
        // live dynamic spec's max_rows_per_batch. Each batch makes the full trip
        // reader → transform_a → swift_map_a → transform_b → swift_map_b → transform_a →
        // reducer; the reducer's external state is carried across batches by hand, since every
        // harness call is stateless. The count must end at exactly 1000.
        Optional<Payload> state = Optional.empty();
        int total = 1000;
        int batchSize = 30;
        for (int fed = 0; fed < total; fed += batchSize) {
            int size = Math.min(batchSize, total - fed);
            var batch = new ArrayList<ExtendedMessage>();
            for (int i = 0; i < size; i++) {
                batch.add(dataMessage("input", "payload"));
            }

            var messages = step("reader", batch);
            messages = step("transform_a", messages);
            assertTrue(messages.stream().allMatch(m -> m.getStreamId().equals("ta1")));
            messages = step("swift_map_a", messages);
            messages = step("transform_b", messages);
            messages = step("swift_map_b", messages);
            messages = step("transform_a", messages);
            assertTrue(messages.stream().allMatch(m -> m.getStreamId().equals("ta2")));
            assertEquals(size, messages.size());

            var request = TestDoProcessRequest.builder("reducer").setMessages(messages);
            state.ifPresent(payload ->
                    request.setState(Reducer.COUNT_STATE, keyOf("payload"), payload));
            var response = harness.doProcess(request.build());
            state = response.allStates().get(Reducer.COUNT_STATE, keyOf("payload")).get();
        }

        assertEquals(1000L, state
                .map(payload -> payload.get("count", Long.class))
                .orElse(null));
    }
}
