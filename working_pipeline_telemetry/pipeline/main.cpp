// A pipeline binary of your own: the runner, the random connector and two user computations.
//
// TReader is a swift ordered source over the built-in NYT::NFlow::TRandomSource. It forwards every
// message downstream, except that it throws on messages whose key equals the spec-injected
// `fail_key`, tagging the error with `fail_comment` — a deliberate, recognizable job failure. The
// scenario's subject is the *engine's* telemetry about that failure and about the moving data:
// the failure comment surfacing in `describe-pipeline`, and the buffer/epoch statistics in the
// flow view.
//
// TProcessor is a transform that consumes the stream and drops it — it exists so the pipeline has
// an inter-computation stream whose buffers and stores the flow view can report on.

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NFlow::NDemo {

using namespace NYT::NTableClient;

////////////////////////////////////////////////////////////////////////////////

struct TReaderParameters
    : public virtual TSwiftOrderedSourceComputation::TParameters
{
    std::string FailKey;
    std::string FailComment;

    REGISTER_YSON_STRUCT(TReaderParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("fail_key", &TThis::FailKey)
            .Default();
        registrar.Parameter("fail_comment", &TThis::FailComment)
            .Default();
    }
};

class TReader
    : public TSwiftOrderedSourceComputation
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TReaderParameters);

    using TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation;

    static inline TStreamId OutputStreamId = TStreamId("data");

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        auto key = GetColumnValue<TStringBuf>(message, "key");
        if (!GetParameters()->FailKey.empty() && key == GetParameters()->FailKey) {
            THROW_ERROR_EXCEPTION("Got fail key %v. Comment: %v", key, GetParameters()->FailComment);
        }

        auto builder = MakeOutputMessageBuilder(OutputStreamId);
        builder.Payload().SetValue(MakeUnversionedStringValue(key), "key");
        builder.Payload().SetValue(MakeUnversionedStringValue(GetColumnValue<TStringBuf>(message, "data")), "data");
        output->AddMessage(builder.Finish());
    }
};

YT_FLOW_DEFINE_COMPUTATION(TReader);

////////////////////////////////////////////////////////////////////////////////

class TProcessor
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoProcessMessage(const TMessage& /*message*/, IOutputCollectorPtr /*output*/) override
    { }
};

YT_FLOW_DEFINE_COMPUTATION(TProcessor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDemo

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
