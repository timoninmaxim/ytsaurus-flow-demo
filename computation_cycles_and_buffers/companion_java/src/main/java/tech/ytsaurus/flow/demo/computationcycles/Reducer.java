package tech.ytsaurus.flow.demo.computationcycles;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.BatchFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.flow.state.ExternalStateDescriptor;
import tech.ytsaurus.flow.state.StateDescriptors;

/**
 * Counts, per key, how many messages came out of the cycle. The count in the external state
 * table is the scenario's assertion: it must equal the number of input messages exactly.
 *
 * <p>As in the Python and Go variants — and unlike the C++ keyed-batch adapter, whose
 * {@code ProcessKey} is called per key — {@code onMessages} gets the request's whole batch with
 * keys mixed, so the reducer groups by key itself. The grouping uses a {@link LinkedHashMap}:
 * groups are processed in first-appearance order, never in map-hash order. There is effectively
 * one key in this scenario, so the group is the batch.
 */
public class Reducer implements BatchFunction {

    static final ExternalStateDescriptor COUNT_STATE = StateDescriptors.external("/state");

    @Override
    public void onMessages(List<ExtendedMessage> messages, OutputCollector output, RuntimeContext ctx) {
        Map<String, List<ExtendedMessage>> groups = new LinkedHashMap<>();
        for (ExtendedMessage message : messages) {
            String data = message.getKey().get("data", String.class);
            groups.computeIfAbsent(data, unused -> new ArrayList<>()).add(message);
        }

        for (List<ExtendedMessage> group : groups.values()) {
            var state = ctx.getState(COUNT_STATE, group.get(0));

            // The C++ variant sleeps 10 ms per message here; kept — it is part of what makes
            // the pipeline slow enough for the cut-buffers pause to catch it mid-flight.
            try {
                Thread.sleep(10L * group.size());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while pacing the reducer", e);
            }

            // A key with no count yet is not always an absent row: live, the state manager hands
            // the companion a present row with the key columns set and "count" null, so the null
            // check must be per column, as in the C++ variant's optional<i64>.value_or(0).
            // getOrDefault() covers the truly-absent case with an all-null row of the state schema.
            Payload row = state.getOrDefault();
            Long count = row.get("count", Long.class);
            // Only "count" is set in the written-back row; the state manager fills the key
            // columns from the grouping key, as in the C++ variant.
            state.set(new PayloadBuilder(row.getSchema())
                    .set("count", (count == null ? 0 : count) + group.size())
                    .finish());
        }
    }
}
