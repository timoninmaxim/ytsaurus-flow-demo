package tech.ytsaurus.flow.demo.swiftmapbatching;

import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.pipeline.FlowApplication;

/**
 * One entry point for both roles, selected by {@code YT_FLOW_MODE}: with the variable unset it
 * is the runner (enriches the spec, ships the companion jars, execs {@code flow_server}); inside
 * the worker's vanilla job it serves the {@code batcher} and {@code writer} computations over
 * the companion gRPC protocol.
 *
 * <p>Only those two computations are registered here — the native {@code reader} runs in-process
 * in the stock {@code flow_server} worker and never calls the companion.
 */
public final class SwiftMapBatchingMain {

    private SwiftMapBatchingMain() {
    }

    public static void main(String[] args) throws Exception {
        var context = new PipelineContext();
        context.registerComputation(Computation.builder()
                .setComputationId("batcher")
                .setProcessFunction(new Batcher())
                .build());
        context.registerComputation(Computation.builder()
                .setComputationId("writer")
                .setProcessFunction(new Writer())
                .build());
        FlowApplication.run(args, context);
    }
}
