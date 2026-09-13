package tech.ytsaurus.flow.demo.telemetry;

import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Consumes the "data" stream and drops it, sleeping {@code sleep_per_message_ms} per message so
 * the processor's input buffer visibly holds data — which is what the flow-view checks read.
 */
public class SleepyDrop implements RowFunction {

    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        YTreeNode sleepMs = ctx.getComputationParameters().get("sleep_per_message_ms");
        if (sleepMs != null && sleepMs.longValue() > 0) {
            try {
                Thread.sleep(sleepMs.longValue());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }
}
