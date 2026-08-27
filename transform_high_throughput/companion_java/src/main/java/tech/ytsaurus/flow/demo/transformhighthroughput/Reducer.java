package tech.ytsaurus.flow.demo.transformhighthroughput;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.BatchFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.state.InternalStateDescriptor;
import tech.ytsaurus.flow.state.StateAccessor;
import tech.ytsaurus.flow.state.StateDescriptors;

/**
 * The per-key-state reducer from ../pipeline/main.cpp re-expressed with the Flow Java SDK —
 * the transform-path benchmark body. Per message it loads the state for the message's key,
 * bumps {@code count}, remembers the payload as {@code last_data}, and re-emits the message,
 * so one message loads the whole transform write path: input store, output store, per-key
 * state, and the output queue behind the async queue sink.
 *
 * <p>The C++ variant's keyed-batch host groups the epoch's input by key before invoking the
 * function; the Java {@link BatchFunction} — like the Go and Python ones — gets the request's
 * whole batch with keys mixed, so the grouping happens here, in first-appearance order (a
 * {@link LinkedHashMap}, never iterating an unordered map — the server may replay a batch and
 * the output order must be deterministic). Applying a group of size N as {@code count += N}
 * with {@code last_data} from the group's last message leaves the same state a per-message
 * loop would — messages within a key preserve their order.
 */
public class Reducer implements BatchFunction {
    static final InternalStateDescriptor<ReducerState> STATE =
            StateDescriptors.yson("state", ReducerState.class);

    @Override
    public void onMessages(List<ExtendedMessage> messages, OutputCollector output, RuntimeContext ctx) {
        Map<String, List<ExtendedMessage>> groups = new LinkedHashMap<>();
        for (ExtendedMessage message : messages) {
            String key = message.getKey().get("key", String.class);
            groups.computeIfAbsent(key, unused -> new ArrayList<>()).add(message);
        }

        for (List<ExtendedMessage> group : groups.values()) {
            StateAccessor<ReducerState> accessor = ctx.getState(STATE, group.get(0));
            ReducerState state = accessor.get().orElseGet(ReducerState::new);

            String lastData = null;
            for (ExtendedMessage message : group) {
                String data = message.get("data", String.class);
                output.addMessage(ctx.createMessageBuilder("out")
                        .set("key", message.get("key", String.class))
                        .set("data", data)
                        .finish());
                lastData = data;
            }

            state.setCount(state.getCount() + group.size());
            state.setLastData(lastData);
            accessor.set(state);
        }
    }
}
