package tech.ytsaurus.flow.demo.statejoiner;

import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.flow.state.ExternalStateDescriptor;
import tech.ytsaurus.flow.state.StateDescriptors;

/**
 * Sums each user's "Amount" into the per-user external state {@code /user_total} — backed by the
 * user_totals table — and forwards the user id (with a constant bucket) into the "users" stream.
 *
 * <p>The mutable side of the join: what this function writes is exactly what
 * {@link JoinerFunction} later reads back through the read-only joined external state.
 */
public class AccumulatorFunction implements RowFunction {

    static final ExternalStateDescriptor TOTAL_STATE = StateDescriptors.external("/user_total");

    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        var state = ctx.getState(TOTAL_STATE, message);
        // A user with no total yet is not always an absent row: live, the state manager hands
        // the companion a present row with the key columns set and "Total" null, so the null
        // check must be per column, as in the C++ variant's optional<i64>.value_or(0).
        // getOrDefault() covers the truly-absent case with an all-null row of the state schema.
        Payload row = state.getOrDefault();
        Long total = row.get("Total", Long.class);
        long amount = message.get("Amount", Long.class);
        // Only "Total" is set in the written-back row; the state manager fills the key columns
        // from the grouping key, as in the C++ variant.
        state.set(new PayloadBuilder(row.getSchema())
                .set("Total", (total == null ? 0 : total) + amount)
                .finish());

        output.addMessage(ctx.createMessageBuilder("users")
                .set("UserId", message.get("UserId", String.class))
                .set("Bucket", 0L)
                .finish());
    }
}
