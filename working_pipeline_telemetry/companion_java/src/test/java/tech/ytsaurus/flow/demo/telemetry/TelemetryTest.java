package tech.ytsaurus.flow.demo.telemetry;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.computation.SourceComputation;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.flow.testutils.TestComputationHarness;
import tech.ytsaurus.flow.testutils.TestDoProcessRequest;
import tech.ytsaurus.typeinfo.TiType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Offline proof of the injection logic through {@link TestComputationHarness} — no cluster
 * needed, mirroring the Go variant's {@code main_test.go}: the bounded raise-then-pass behaviour
 * of both failure shapes (a thrown {@link RuntimeException} and a thrown {@link AssertionError}),
 * the per-row budget isolation and the passthrough are pinned here; the live run then only
 * re-checks the engine-telemetry asserts.
 *
 * <p>The failure budget is process-local by design (see {@link FailingRead}), so every test uses
 * its own unique "data" values instead of resetting state — exactly how live fail rows stay
 * independent. The test spec's {@code fail_attempts} is 3.
 */
public class TelemetryTest {

    private static final String FAIL_COMMENT = "TELEMETRY_DEMO_INTENTIONAL_FAIL";
    private static final String FAIL_KEY = "1100";
    private static final String ERROR_KEY = "1101";
    private static final int FAIL_ATTEMPTS = 3;

    private static final TableSchema ROW_SCHEMA = TableSchema.builder()
            .addValue("key", TiType.string())
            .addValue("data", TiType.string())
            .build();

    private static final TableSchema KEY_SCHEMA = TableSchema.builder()
            .addValue("hash", TiType.uint64())
            .addValue("key", TiType.string())
            .build();

    private TestComputationHarness harness;

    @BeforeEach
    public void setUp() {
        var context = new PipelineContext();
        context.registerComputation(SourceComputation.builder()
                .setComputationId("reader")
                .setProcessFunction(new FailingRead())
                .build());
        context.registerComputation(Computation.builder()
                .setComputationId("processor")
                .setProcessFunction(new SleepyDrop())
                .build());
        harness = TestComputationHarness.builder()
                .setPipelineContext(context)
                .setPipelineSpec(getClass().getClassLoader().getResourceAsStream("pipeline.yson"))
                .build();
    }

    private static ExtendedMessage rowMessage(String streamId, String key, String data) {
        return ExtendedMessage.builder()
                .setMessageId(GUID.create().toString())
                .setStreamId(streamId)
                .setKey(new PayloadBuilder(KEY_SCHEMA)
                        .set("hash", 0L)
                        .set("key", key)
                        .finish())
                .setPayload(new PayloadBuilder(ROW_SCHEMA)
                        .set("key", key)
                        .set("data", data)
                        .finish())
                .build();
    }

    private TestDoProcessRequest readerRequest(String key, String data) {
        return TestDoProcessRequest.builder("reader")
                .setMessages(List.of(rowMessage("input", key, data)))
                .build();
    }

    @Test
    public void ordinaryKeyPassesThrough() {
        var response = harness.doProcess(readerRequest("7", "payload-passthrough"));

        var messages = response.getOutputMessagesFlatten();
        assertEquals(1, messages.size());
        assertEquals("data", messages.get(0).getStreamId());
        assertEquals("7", messages.get(0).get("key", String.class));
        assertEquals("payload-passthrough", messages.get(0).get("data", String.class));
    }

    @Test
    public void failKeyThrowsExactlyFailAttemptsTimesThenPasses() {
        for (int attempt = 1; attempt <= FAIL_ATTEMPTS; attempt++) {
            var ex = assertThrows(RuntimeException.class,
                    () -> harness.doProcess(readerRequest(FAIL_KEY, "fail-budget-row")));
            assertNotNull(ex.getMessage(), "attempt " + attempt);
            assertTrue(ex.getMessage().contains(FAIL_COMMENT), "attempt " + attempt);
            assertTrue(ex.getMessage().contains("Got fail key " + FAIL_KEY), "attempt " + attempt);
        }

        var response = harness.doProcess(readerRequest(FAIL_KEY, "fail-budget-row"));
        assertEquals(1, response.getOutputMessagesFlatten().size());
    }

    @Test
    public void errorKeyThrowsErrorExactlyFailAttemptsTimesThenPasses() {
        for (int attempt = 1; attempt <= FAIL_ATTEMPTS; attempt++) {
            var error = assertThrows(AssertionError.class,
                    () -> harness.doProcess(readerRequest(ERROR_KEY, "error-budget-row")));
            assertNotNull(error.getMessage(), "attempt " + attempt);
            assertTrue(error.getMessage().contains(FAIL_COMMENT), "attempt " + attempt);
            assertTrue(error.getMessage().contains("Got error key " + ERROR_KEY), "attempt " + attempt);
        }

        var response = harness.doProcess(readerRequest(ERROR_KEY, "error-budget-row"));
        assertEquals(1, response.getOutputMessagesFlatten().size());
    }

    @Test
    public void eachFailRowGetsItsOwnBudget() {
        assertThrows(RuntimeException.class,
                () -> harness.doProcess(readerRequest(FAIL_KEY, "budget-row-a")));
        // A different fail row starts its own count even though the first is mid-budget.
        assertThrows(RuntimeException.class,
                () -> harness.doProcess(readerRequest(FAIL_KEY, "budget-row-b")));
    }

    @Test
    public void processorDropsEverything() {
        var response = harness.doProcess(TestDoProcessRequest.builder("processor")
                .setMessages(List.of(
                        rowMessage("data", "7", "x"),
                        rowMessage("data", "8", "y")))
                .build());

        assertTrue(response.getOutputMessagesFlatten().isEmpty());
    }
}
