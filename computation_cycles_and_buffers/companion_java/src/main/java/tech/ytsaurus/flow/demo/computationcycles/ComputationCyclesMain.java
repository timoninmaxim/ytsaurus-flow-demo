package tech.ytsaurus.flow.demo.computationcycles;

import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.computation.SourceComputation;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.pipeline.FlowApplication;

/**
 * One entry point for both roles, selected by {@code YT_FLOW_MODE}: with the variable unset it is
 * the runner (enriches the spec, ships the companion jars, execs {@code flow_server}); inside the
 * worker's vanilla job it serves all six computations over the companion gRPC protocol.
 *
 * <p>The four cycle computations are one {@link CyclePassthrough} function under four computation
 * ids — the Java counterpart of registering {@code TCyclePassthroughFunction} four times in the
 * C++ companion. Which of them is a transform and which is a swift map is decided by the host
 * class the spec names; the SDK dispatches by computation id only.
 */
public final class ComputationCyclesMain {

    private ComputationCyclesMain() {
    }

    public static void main(String[] args) throws Exception {
        var context = new PipelineContext();
        context.registerComputation(SourceComputation.builder()
                .setComputationId("reader")
                .setProcessFunction(new ReadData())
                .build());
        for (String computationId : new String[]{
                "transform_a", "swift_map_a", "transform_b", "swift_map_b"}) {
            context.registerComputation(Computation.builder()
                    .setComputationId(computationId)
                    .setProcessFunction(new CyclePassthrough())
                    .build());
        }
        context.registerComputation(Computation.builder()
                .setComputationId("reducer")
                .setProcessFunction(new Reducer())
                .build());
        FlowApplication.run(args, context);
    }
}
