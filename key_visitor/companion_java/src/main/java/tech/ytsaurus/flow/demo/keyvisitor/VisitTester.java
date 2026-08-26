package tech.ytsaurus.flow.demo.keyvisitor;

import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Visit;
import tech.ytsaurus.flow.state.InternalStateDescriptor;
import tech.ytsaurus.flow.state.StateAccessor;
import tech.ytsaurus.flow.state.StateDescriptors;

/**
 * The visit tester from ../pipeline/main.cpp re-expressed with the Flow Java SDK.
 *
 * <p>Only what a visit sees is user code; the engine drives everything else (key tracking, the
 * periodic sweep, the final pass after the finite source drains) identically to the C++, Python
 * and Go variants:
 *
 * <ul>
 * <li>on a message, store its payload in the per-key internal state {@code user_state}
 *     (declared in the spec's {@code parameters/internal_states});
 * <li>on a visit, emit the <em>stored</em> payload together with a per-key visit counter,
 *     or nothing if the key has no state yet.
 * </ul>
 */
public class VisitTester implements RowFunction {
    static final InternalStateDescriptor<UserState> USER_STATE =
            StateDescriptors.yson("user_state", UserState.class);

    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        StateAccessor<UserState> accessor = ctx.getState(USER_STATE, message);
        // Keep the previously stored visit counter; only the payload changes.
        UserState state = accessor.get().orElseGet(UserState::new);
        state.setPayload(message.get("payload", String.class));
        accessor.set(state);
    }

    @Override
    public void onVisit(Visit visit, OutputCollector output, RuntimeContext ctx) {
        StateAccessor<UserState> accessor = ctx.getState(USER_STATE, visit);
        UserState state = accessor.get().orElse(null);
        if (state == null) {
            return;
        }
        state.setVisitIndex(state.getVisitIndex() + 1);
        accessor.set(state);

        output.addMessage(ctx.createMessageBuilder("visits")
                .set("key", visit.getKey().get("key", String.class))
                .set("payload", state.getPayload())
                .set("visit_index", state.getVisitIndex())
                .finish());
    }
}
