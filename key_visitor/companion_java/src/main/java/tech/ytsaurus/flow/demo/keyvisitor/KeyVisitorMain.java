package tech.ytsaurus.flow.demo.keyvisitor;

import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.pipeline.FlowApplication;

/**
 * One entry point for both roles, selected by {@code YT_FLOW_MODE}: with the variable unset it is
 * the runner (enriches the spec, ships the companion jars, execs {@code flow_server}); inside the
 * worker's vanilla job it serves the {@code tester} computation over the companion gRPC protocol.
 *
 * <p>Only the {@code tester} computation is registered here — the native {@code key_reader} runs
 * in-process in the stock {@code flow_server} worker and never calls the companion.
 */
public final class KeyVisitorMain {

    private KeyVisitorMain() {
    }

    public static void main(String[] args) throws Exception {
        var context = new PipelineContext();
        context.registerComputation(Computation.builder()
                .setComputationId("tester")
                .setProcessFunction(new VisitTester())
                .build());
        FlowApplication.run(args, context);
    }
}
