package tech.ytsaurus.flow.demo.transformhighthroughput;

import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.pipeline.FlowApplication;

/**
 * One entry point for both roles, selected by {@code YT_FLOW_MODE}: with the variable unset it
 * is the runner (enriches the spec, ships the companion jars, execs {@code flow_server}); inside
 * the worker's vanilla job it serves the {@code Reducer} computation over the companion gRPC
 * protocol.
 *
 * <p>Only the {@code Reducer} computation is registered here — the native {@code Reader} runs
 * in-process in the stock {@code flow_server} worker and never calls the companion, so the Java
 * code sits exactly where the C++ user code sat: on the transform path.
 */
public final class TransformHighThroughputMain {

    private TransformHighThroughputMain() {
    }

    public static void main(String[] args) throws Exception {
        var context = new PipelineContext();
        context.registerComputation(Computation.builder()
                .setComputationId("Reducer")
                .setProcessFunction(new Reducer())
                .build());
        FlowApplication.run(args, context);
    }
}
