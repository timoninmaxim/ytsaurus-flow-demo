package tech.ytsaurus.flow.demo.standalonedirect;

import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.pipeline.FlowApplication;

/**
 * One entry point for both roles, selected by {@code YT_FLOW_MODE}: with the variable unset it is
 * the launcher (enriches the spec, execs {@code flow_server}); inside the worker container it
 * serves the "writer" computation over the companion gRPC protocol. Unlike a vanilla launch, the
 * worker here spawns this same class directly from the container's own filesystem -- see the
 * scenario README's "How the companion gets into the job".
 */
public final class StandaloneDirectMain {

    private StandaloneDirectMain() {
    }

    public static void main(String[] args) throws Exception {
        var context = new PipelineContext();
        context.registerComputation(Computation.builder()
                .setComputationId("writer")
                .setProcessFunction(new MessageFilter())
                .build());
        FlowApplication.run(args, context);
    }
}
