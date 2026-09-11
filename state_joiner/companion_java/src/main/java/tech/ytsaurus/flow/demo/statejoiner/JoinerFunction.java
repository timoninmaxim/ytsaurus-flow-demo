package tech.ytsaurus.flow.demo.statejoiner;

import java.util.List;

import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.BatchFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.state.JoinedExternalStateDescriptor;
import tech.ytsaurus.flow.state.ReadOnlyExternalStateAccessor;
import tech.ytsaurus.flow.state.StateDescriptors;

/**
 * Joins the totals {@link AccumulatorFunction} keeps in the user_totals table — reached through
 * the read-only joined external state {@code /user_total} declared under the computation's
 * {@code external_state_joiners} — and emits the stored total for each incoming user into the
 * "results" stream.
 *
 * <p>A {@link BatchFunction}, matching the {@code IBatchProcessFunction} granularity of the
 * upstream test's joiner. The batch arrives keys mixed, which is fine here: the join key is the
 * message's own key ({@code join_on = {}}) and each message is handled on its own.
 *
 * <p>"Joiners never write back" is enforced at runtime: the accessor is a
 * {@link ReadOnlyExternalStateAccessor}, whose {@code set()} and {@code clear()} throw.
 */
public class JoinerFunction implements BatchFunction {

    static final JoinedExternalStateDescriptor TOTAL_STATE =
            StateDescriptors.externalReadOnly("/user_total");

    @Override
    public void onMessages(List<ExtendedMessage> messages, OutputCollector output, RuntimeContext ctx) {
        for (ExtendedMessage message : messages) {
            ReadOnlyExternalStateAccessor state = ctx.getState(TOTAL_STATE, message);
            // A key with no row in the joined table arrives as an all-null state (the worker-side
            // preload keeps missing rows), a key the batch carried nothing for as an absent one.
            // Report either as -1 instead of throwing: an exception thrown in a companion is
            // retried forever, whereas a sentinel in the output table makes a broken join visible
            // at a glance.
            Long total = state.get()
                    .map(payload -> payload.get("Total", Long.class))
                    .orElse(null);

            output.addMessage(ctx.createMessageBuilder("results")
                    .set("UserId", message.get("UserId", String.class))
                    .set("Total", total == null ? -1L : total)
                    .finish());
        }
    }
}
