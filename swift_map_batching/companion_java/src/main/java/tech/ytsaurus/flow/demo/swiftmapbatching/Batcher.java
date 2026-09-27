package tech.ytsaurus.flow.demo.swiftmapbatching;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.BatchFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;

/**
 * Merges the epoch's messages of one key into a single output message carrying their
 * comma-joined event ids. Hosted by {@code TSwiftMapCompanionComputation}: each merged output
 * has as many parents as its key had messages, which the swift map accepts only under
 * {@code allow_batching_with_relaxed_guarantees}.
 *
 * <p>As in the Python and Go SDKs — and unlike the C++ keyed-batch adapter — {@code onMessages}
 * gets the request's whole message batch, keys mixed, with all of it as the default parent set.
 * So the batcher groups by key itself and binds each merged output to exactly its key group
 * through the collector {@link OutputCollector#setParentIds(List)} returns.
 *
 * <p>Two determinism obligations of swift hosting, and how they are met:
 *
 * <ul>
 * <li>the companion gRPC server may call user functions concurrently across requests, so the
 *     function keeps no mutable state — every call works only on its own batch and collectors;
 * <li>within a request the output must be a pure function of the batch, groups and parents in
 *     a reproducible order. The grouping therefore uses a {@link LinkedHashMap}: group order is
 *     first-appearance order, and each group's parent ids and event ids stay in batch order.
 * </ul>
 */
public class Batcher implements BatchFunction {

    @Override
    public void onMessages(List<ExtendedMessage> messages, OutputCollector output, RuntimeContext ctx) {
        Map<Long, List<ExtendedMessage>> groups = new LinkedHashMap<>();
        for (ExtendedMessage message : messages) {
            Long groupKey = message.getKey().get("group_key", Long.class);
            groups.computeIfAbsent(groupKey, unused -> new java.util.ArrayList<>()).add(message);
        }

        for (List<ExtendedMessage> group : groups.values()) {
            String eventIds = group.stream()
                    .map(message -> String.valueOf(message.get("event_id", Long.class)))
                    .collect(Collectors.joining(","));
            List<String> parentIds = group.stream()
                    .map(ExtendedMessage::getMessageId)
                    .toList();

            // A fresh collector per key group: its parents are that key's messages, nothing
            // else. The default collector (parents = the whole mixed-key batch) stays unused.
            output.setParentIds(parentIds).addMessage(ctx.createMessageBuilder("event_batched")
                    .set("event_ids", eventIds)
                    .finish());
        }
    }
}
