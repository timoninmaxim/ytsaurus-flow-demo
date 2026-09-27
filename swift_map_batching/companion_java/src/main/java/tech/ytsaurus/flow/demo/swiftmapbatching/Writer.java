package tech.ytsaurus.flow.demo.swiftmapbatching;

import java.util.ArrayList;
import java.util.List;

import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;

/**
 * Explodes a batched message back into one message per event id, tagging each with the size of
 * the batch it came out of — the only place the merging is visible downstream. Hosted by
 * {@code TTransformCompanionComputation} with {@code processing_mode = exactly_once}.
 */
public class Writer implements RowFunction {

    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        List<Long> eventIds = new ArrayList<>();
        for (String token : message.get("event_ids", String.class).split(",")) {
            if (!token.isEmpty()) {
                eventIds.add(Long.parseLong(token));
            }
        }

        for (Long eventId : eventIds) {
            output.addMessage(ctx.createMessageBuilder("sink_event")
                    .set("event_id", eventId)
                    .set("batch_size", (long) eventIds.size())
                    .finish());
        }
    }
}
