package tech.ytsaurus.flow.demo.statejoiner;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.flow.testutils.TestComputationHarness;
import tech.ytsaurus.flow.testutils.TestDoProcessRequest;
import tech.ytsaurus.flow.testutils.TestDoProcessResponse;
import tech.ytsaurus.typeinfo.TiType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Offline tests of the join logic, mirroring the Go variant's {@code main_test.go}: both
 * computations are driven through {@link TestComputationHarness} — no cluster needed. The
 * engine-side subject (the worker preloading the joined states from the user_totals table and
 * shipping them with the batch, the epoch transaction, the sync sink) is proven by the live run.
 */
public class StateJoinerTest {

    private static final TableSchema KEY_SCHEMA = TableSchema.builder()
            .addValue("Hash", TiType.uint64())
            .addValue("UserId", TiType.string())
            .build();

    private static final TableSchema EVENTS_SCHEMA = TableSchema.builder()
            .addValue("UserId", TiType.string())
            .addValue("Amount", TiType.int64())
            .build();

    private static final TableSchema USERS_SCHEMA = TableSchema.builder()
            .addValue("UserId", TiType.string())
            .addValue("Bucket", TiType.uint64())
            .build();

    // The user_totals table backing the external state "/user_total": keyed exactly by the
    // accumulator's group_by_schema, with the total as the single value column.
    private static final TableSchema STATE_SCHEMA = TableSchema.builder()
            .addValue("Hash", TiType.uint64())
            .addValue("UserId", TiType.string())
            .addValue("Total", TiType.int64())
            .build();

    private TestComputationHarness harness;

    @BeforeEach
    public void setUp() {
        var context = new PipelineContext();
        context.registerComputation(Computation.builder()
                .setComputationId("accumulator")
                .setProcessFunction(new AccumulatorFunction())
                .build());
        context.registerComputation(Computation.builder()
                .setComputationId("joiner")
                .setProcessFunction(new JoinerFunction())
                .build());
        harness = TestComputationHarness.builder()
                .setPipelineContext(context)
                .setPipelineSpec(getClass().getClassLoader().getResourceAsStream("pipeline.yson"))
                .addExternalStateSchema("/user_total", STATE_SCHEMA)
                .build();
    }

    private static Payload keyOf(String userId) {
        // Any deterministic per-user hash works offline; live, the engine computes farm_hash.
        return new PayloadBuilder(KEY_SCHEMA)
                .set("Hash", Integer.toUnsignedLong(userId.hashCode()))
                .set("UserId", userId)
                .finish();
    }

    private static Payload totalOf(String userId, long total) {
        return new PayloadBuilder(STATE_SCHEMA)
                .set("Hash", Integer.toUnsignedLong(userId.hashCode()))
                .set("UserId", userId)
                .set("Total", total)
                .finish();
    }

    private static ExtendedMessage eventMessage(String userId, long amount) {
        return ExtendedMessage.builder()
                .setMessageId(GUID.create().toString())
                .setStreamId("events")
                .setKey(keyOf(userId))
                .setPayload(new PayloadBuilder(EVENTS_SCHEMA)
                        .set("UserId", userId)
                        .set("Amount", amount)
                        .finish())
                .build();
    }

    private static ExtendedMessage userMessage(String userId) {
        return ExtendedMessage.builder()
                .setMessageId(GUID.create().toString())
                .setStreamId("users")
                .setKey(keyOf(userId))
                .setPayload(new PayloadBuilder(USERS_SCHEMA)
                        .set("UserId", userId)
                        .set("Bucket", 0L)
                        .finish())
                .build();
    }

    private Long storedTotal(TestDoProcessResponse response, String userId) {
        return response.allStates().get(AccumulatorFunction.TOTAL_STATE, keyOf(userId)).get()
                .map(payload -> payload.get("Total", Long.class))
                .orElse(null);
    }

    @Test
    public void accumulatorStartsFromZeroAndForwardsTheUser() {
        var response = harness.doProcess(TestDoProcessRequest.builder("accumulator")
                .setMessages(List.of(eventMessage("user-0", 10)))
                .build());

        assertEquals(10L, storedTotal(response, "user-0"));

        var forwarded = response.getOutputMessagesFlatten();
        assertEquals(1, forwarded.size());
        assertEquals("users", forwarded.get(0).getStreamId());
        assertEquals("user-0", forwarded.get(0).get("UserId", String.class));
        assertEquals(0L, forwarded.get(0).get("Bucket", Long.class));
    }

    @Test
    public void accumulatorAddsToTheStoredTotal() {
        var response = harness.doProcess(TestDoProcessRequest.builder("accumulator")
                .setState(AccumulatorFunction.TOTAL_STATE, keyOf("user-1"), totalOf("user-1", 15))
                .setMessages(List.of(eventMessage("user-1", 5)))
                .build());

        assertEquals(20L, storedTotal(response, "user-1"));
    }

    @Test
    public void joinerEmitsTheJoinedTotal() {
        var response = harness.doProcess(TestDoProcessRequest.builder("joiner")
                .setState(JoinerFunction.TOTAL_STATE, keyOf("user-2"), totalOf("user-2", 30))
                .setMessages(List.of(userMessage("user-2")))
                .build());

        var results = response.getOutputMessagesFlatten();
        assertEquals(1, results.size());
        assertEquals("results", results.get(0).getStreamId());
        assertEquals("user-2", results.get(0).get("UserId", String.class));
        assertEquals(30L, results.get(0).get("Total", Long.class));
    }

    @Test
    public void joinerReportsAMissAsMinusOne() {
        // No joined state seeded for the key: live this is the shape of a key the batch carried
        // no state for; a key with no row in the table arrives instead as an all-null state row.
        var response = harness.doProcess(TestDoProcessRequest.builder("joiner")
                .setMessages(List.of(userMessage("user-9")))
                .build());

        var results = response.getOutputMessagesFlatten();
        assertEquals(1, results.size());
        assertEquals(-1L, results.get(0).get("Total", Long.class));
    }

    @Test
    public void joinerReportsAnAllNullRowAsMinusOne() {
        // The truly-absent-row shape: the worker-side preload keeps missing rows, so the key
        // still gets a state — an all-null row of the state schema with "Total" null.
        var allNull = new PayloadBuilder(STATE_SCHEMA).finish();
        var response = harness.doProcess(TestDoProcessRequest.builder("joiner")
                .setState(JoinerFunction.TOTAL_STATE, keyOf("user-3"), allNull)
                .setMessages(List.of(userMessage("user-3")))
                .build());

        var results = response.getOutputMessagesFlatten();
        assertEquals(1, results.size());
        assertEquals(-1L, results.get(0).get("Total", Long.class));
        // The joiner never writes: no state of any kind may come back modified.
        assertTrue(response.modifiedStates().externalNames().isEmpty());
    }
}
