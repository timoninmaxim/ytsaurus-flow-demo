package tech.ytsaurus.flow.demo.wordcountsync;

import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;

/**
 * Splits each input text message into whitespace-separated words and emits one message per word.
 *
 * <p>Hosted by {@code TSwiftOrderedSourceCompanionComputation}: a swift source, so it must be
 * deterministic — {@code String.split} over a fixed pattern is.
 */
public class TextRead implements RowFunction {

    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        String text = message.get("text", String.class);
        if (text == null) {
            return;
        }
        for (String word : text.split("[ \t\n\r]+")) {
            if (word.isEmpty()) {
                continue;
            }
            output.addMessage(ctx.createMessageBuilder("words")
                    .set("word", word)
                    .finish());
        }
    }
}
