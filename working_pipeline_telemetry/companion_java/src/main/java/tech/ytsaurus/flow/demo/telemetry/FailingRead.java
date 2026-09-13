package tech.ytsaurus.flow.demo.telemetry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Forwards each input-queue row into the "data" stream, except that it fails on rows whose key
 * equals the spec-injected {@code fail_key} (throwing a {@link RuntimeException} whose message
 * carries {@code fail_comment}) or {@code error_key} (throwing an {@link AssertionError} — a
 * {@code java.lang.Error}, the failure shape the SDK server's {@code catch (Exception)} around
 * ProcessBatch does <em>not</em> see, so both ways Java user code can fail are exercised).
 *
 * <p>The failure must be transient: the input is a queue, so a row that failed forever would be
 * re-read forever and poison the pipeline (a companion error is retried, first by the worker's
 * gRPC retry loop, then by the restarted job). Hence {@code fail_attempts}: the failure repeats
 * per unique row (keyed by its "data" value, a process-local count — the companion JVM is per
 * worker and survives job restarts) exactly {@code fail_attempts} times and then lets the row
 * pass. The worker's retry budget is {@code invocation_count + 1} attempts, so with the spec's
 * {@code backoff/invocation_count = 5} a {@code fail_attempts} of 8 exhausts the first budget
 * (six failures — one genuine job failure fires), and the restarted job's re-read spends the
 * remaining two failures inside its own budget and passes.
 */
public class FailingRead implements RowFunction {

    /**
     * The process-local failure budget, keyed by the fail row's unique "data" value. Only
     * fail/error-key rows ever get an entry. The server may call the function concurrently
     * across requests, hence the atomic merge.
     */
    private final ConcurrentHashMap<String, Long> failCounts = new ConcurrentHashMap<>();

    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        Map<String, YTreeNode> parameters = ctx.getComputationParameters();
        String key = message.get("key", String.class);
        String data = message.get("data", String.class);

        long failAttempts = longParameter(parameters, "fail_attempts");
        String comment = stringParameter(parameters, "fail_comment");

        String failKey = stringParameter(parameters, "fail_key");
        if (!failKey.isEmpty() && failKey.equals(key)) {
            long attempt = tryTakeFailAttempt(data, failAttempts);
            if (attempt > 0) {
                System.err.printf("read: failing on fail key (data: %s, attempt: %d)%n", data, attempt);
                throw new RuntimeException("Got fail key " + key + ". Comment: " + comment);
            }
        }

        String errorKey = stringParameter(parameters, "error_key");
        if (!errorKey.isEmpty() && errorKey.equals(key)) {
            long attempt = tryTakeFailAttempt(data, failAttempts);
            if (attempt > 0) {
                System.err.printf("read: throwing Error on error key (data: %s, attempt: %d)%n", data, attempt);
                throw new AssertionError("Got error key " + key + ". Comment: " + comment);
            }
        }

        output.addMessage(ctx.createMessageBuilder("data")
                .set("key", key)
                .set("data", data)
                .finish());
    }

    /**
     * Counts one more failure for the row and returns the attempt number, or 0 when the budget
     * is exhausted and the row must pass.
     */
    private long tryTakeFailAttempt(String data, long failAttempts) {
        long[] taken = new long[1];
        failCounts.compute(data, (k, v) -> {
            long current = v == null ? 0 : v;
            if (current >= failAttempts) {
                taken[0] = 0;
                return current;
            }
            taken[0] = current + 1;
            return current + 1;
        });
        return taken[0];
    }

    private static String stringParameter(Map<String, YTreeNode> parameters, String name) {
        YTreeNode node = parameters.get(name);
        return node == null ? "" : node.stringValue();
    }

    private static long longParameter(Map<String, YTreeNode> parameters, String name) {
        YTreeNode node = parameters.get(name);
        return node == null ? 0 : node.longValue();
    }
}
