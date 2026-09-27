package tech.ytsaurus.flow.demo.computationcycles;

import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Forwards every message to the output stream its input stream maps to under the
 * "passthrough_rules" parameter, sleeping "sleep_per_message" milliseconds first. All four
 * computations of the cycle are this one function under different parameters; which of them is a
 * transform and which is a swift map is decided by the host class in the spec. The cycle itself
 * is pure spec topology (input/output stream ids and streams_dependency); the function only picks
 * the output stream, and transform_a is where the routing matters — reader_output goes once
 * around the loop (→ ta1) and sb1 releases the message to the reducer on the way back (→ ta2).
 *
 * <p>The two computations the spec hosts with {@code TSwiftMapCompanionComputation} must be
 * deterministic: the passthrough is — its output depends only on the input message and the spec
 * parameters, and the sleep does not shape the output. The companion gRPC server may call the
 * function concurrently across requests, so it keeps no mutable state.
 *
 * <p>An input stream missing from the rules is an error: the topology of this scenario is the
 * point, so a message with nowhere to go must be reported rather than dropped. (An exception
 * from a process function is retried forever — see word_count_sync's README.)
 */
public class CyclePassthrough implements RowFunction {

    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        var parameters = ctx.getComputationParameters();

        // Artificial per-message delay, by input stream, in milliseconds: slows the cycle down
        // enough for the buffers between its computations to fill.
        YTreeNode sleeps = parameters.get("sleep_per_message");
        if (sleeps != null) {
            YTreeNode sleep = sleeps.asMap().get(message.getStreamId());
            if (sleep != null && sleep.longValue() > 0) {
                try {
                    Thread.sleep(sleep.longValue());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while pacing the cycle", e);
                }
            }
        }

        YTreeNode rules = parameters.get("passthrough_rules");
        YTreeNode outputStream = rules == null ? null : rules.asMap().get(message.getStreamId());
        if (outputStream == null) {
            throw new IllegalStateException(
                    "No passthrough rule for input stream \"" + message.getStreamId() + "\"");
        }

        output.addMessage(ctx.createMessageBuilder(outputStream.stringValue())
                .set("data", message.get("data", String.class))
                .finish());
    }
}
