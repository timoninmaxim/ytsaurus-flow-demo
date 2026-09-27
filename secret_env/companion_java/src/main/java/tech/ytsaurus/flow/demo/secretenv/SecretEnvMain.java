package tech.ytsaurus.flow.demo.secretenv;

import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.pipeline.FlowApplication;

/**
 * One entry point for both roles, selected by {@code YT_FLOW_MODE}: with the variable unset it is
 * the runner (enriches the spec, ships the companion jars, spawns {@code flow_server}); inside the
 * worker's vanilla job it serves the {@code checker} computation over the companion gRPC protocol.
 *
 * <p>Only {@code checker} is registered here — the native {@code reader} runs in-process in the
 * stock {@code flow_server} worker and never calls the companion.
 */
public final class SecretEnvMain {

    private SecretEnvMain() {
    }

    public static void main(String[] args) throws Exception {
        var context = new PipelineContext();
        context.registerComputation(Computation.builder()
                .setComputationId("checker")
                .setProcessFunction(new SecretCheckerFunction())
                .build());
        FlowApplication.run(args, context);
    }
}
