package tech.ytsaurus.flow.demo.statejoiner;

import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.pipeline.FlowApplication;

/**
 * One entry point for both roles, selected by {@code YT_FLOW_MODE}: with the variable unset it is
 * the runner (enriches the spec, ships the companion jars, execs {@code flow_server}); inside the
 * worker's vanilla job it serves the {@code accumulator} and {@code joiner} computations over the
 * companion gRPC protocol.
 *
 * <p>Only those two are registered here — the native {@code reader} runs in-process in the stock
 * {@code flow_server} worker and never calls the companion. One companion process backs
 * computations that use different state facilities: a mutable external state manager in the
 * accumulator, a read-only external state joiner in the joiner.
 */
public final class StateJoinerMain {

    private StateJoinerMain() {
    }

    public static void main(String[] args) throws Exception {
        var context = new PipelineContext();
        context.registerComputation(Computation.builder()
                .setComputationId("accumulator")
                .setProcessFunction(new AccumulatorFunction())
                .build());
        context.registerComputation(Computation.builder()
                .setComputationId("joiner")
                .setProcessFunction(new JoinerFunction())
                .build());
        FlowApplication.run(args, context);
    }
}
