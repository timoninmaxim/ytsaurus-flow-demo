package tech.ytsaurus.flow.demo.wordcountsync;

import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.computation.SourceComputation;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.pipeline.FlowApplication;

/**
 * One entry point for both roles, selected by {@code YT_FLOW_MODE}: with the variable unset it is
 * the runner (enriches the spec, ships the companion jars, execs {@code flow_server}); inside the
 * worker's vanilla job it serves both computations over the companion gRPC protocol.
 *
 * <p>Unlike the {@code key_visitor} Java variant, the reader is registered here too: it is a
 * companion <em>source</em> ({@link SourceComputation}), hosted by the spec's
 * {@code NCompanion::TSwiftOrderedSourceCompanionComputation} — this variant puts a Java function
 * on the source path, as the Go variant does.
 */
public final class WordCountSyncMain {

    private WordCountSyncMain() {
    }

    public static void main(String[] args) throws Exception {
        var context = new PipelineContext();
        context.registerComputation(SourceComputation.builder()
                .setComputationId("reader")
                .setProcessFunction(new TextRead())
                .build());
        context.registerComputation(Computation.builder()
                .setComputationId("counter")
                .setProcessFunction(new WordCount())
                .build());
        FlowApplication.run(args, context);
    }
}
