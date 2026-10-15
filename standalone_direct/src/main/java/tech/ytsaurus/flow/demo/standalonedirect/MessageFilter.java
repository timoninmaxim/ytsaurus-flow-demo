package tech.ytsaurus.flow.demo.standalonedirect;

import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;

/**
 * Drops rows whose {@code key} is blacklisted ({@code "bad"}) and, for the rest, adds
 * {@code data_upper} -- proof the row went through this Java function rather than the stock C++
 * {@code skip_if_expression} / passthrough it replaces.
 *
 * <p>Hosted by {@code TTransformCompanionComputation}: a keyed transform, grouped the same way
 * the C++ {@code TPassthroughComputation} it replaces was (group_by_schema hash/key).
 */
public class MessageFilter implements RowFunction {

    private static final String BLACKLISTED_KEY = "bad";

    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        String key = message.get("key", String.class);
        if (BLACKLISTED_KEY.equals(key)) {
            return;
        }

        String data = message.get("data", String.class);
        output.addMessage(ctx.createMessageBuilder("event_out")
                .set("key", key)
                .set("data", data)
                .set("data_upper", data.toUpperCase())
                .finish());
    }
}
