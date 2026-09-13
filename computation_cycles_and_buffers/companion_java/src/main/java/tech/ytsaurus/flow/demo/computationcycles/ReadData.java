package tech.ytsaurus.flow.demo.computationcycles;

import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;

/**
 * Reads the input queue and republishes the "data" column into "reader_output".
 *
 * <p>Hosted by {@code TSwiftOrderedSourceCompanionComputation}: a swift source, so it must be
 * deterministic — a column copy is.
 */
public class ReadData implements RowFunction {

    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        output.addMessage(ctx.createMessageBuilder("reader_output")
                .set("data", message.get("data", String.class))
                .finish());
    }
}
