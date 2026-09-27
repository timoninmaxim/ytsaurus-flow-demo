package tech.ytsaurus.flow.demo.telemetry;

import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.computation.SourceComputation;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.pipeline.FlowApplication;

/**
 * One entry point for both roles, selected by {@code YT_FLOW_MODE}: with the variable unset it is
 * the runner (enriches the spec, ships the companion jars, spawns {@code flow_server}); inside the
 * worker's vanilla job it serves both computations over the companion gRPC protocol.
 *
 * <p>The reader is a companion <em>source</em> ({@link SourceComputation}) hosted by the spec's
 * {@code NCompanion::TSwiftOrderedSourceCompanionComputation}, as in the {@code word_count_sync}
 * Java variant; the processor is an ordinary {@link Computation} behind
 * {@code TTransformCompanionComputation}.
 */
public final class TelemetryMain {

    private TelemetryMain() {
    }

    public static void main(String[] args) throws Exception {
        var context = new PipelineContext();
        context.registerComputation(SourceComputation.builder()
                .setComputationId("reader")
                .setProcessFunction(new FailingRead())
                .build());
        context.registerComputation(Computation.builder()
                .setComputationId("processor")
                .setProcessFunction(new SleepyDrop())
                .build());
        FlowApplication.run(args, context);
    }
}
