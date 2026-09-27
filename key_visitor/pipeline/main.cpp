// A pipeline binary of your own: the runner plus the visit-tester process function.
//
// The scenario's subject is the engine's **key-visitor stream**: the worker sweeps every key the
// `tester` computation has state for and injects a visit per key into the computation, on top of
// the ordinary message flow. The user code below only proves what a visit sees:
//
// - TVisitTesterFunction.ProcessMessage stores the message payload in per-key internal state;
// - TVisitTesterFunction.ProcessVisit emits the *stored* payload with a per-key visit counter.
//
// Messages and visits are ports of yt/yt/flow/tests/key_visitor/cpp/lib (internal-state variant),
// with the class names moved to the demo namespace.

#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow::NDemo {

////////////////////////////////////////////////////////////////////////////////

//! Input messages keyed by `key` carry an arbitrary payload that the function stores in its
//! per-key state.
struct TKeyMessage
    : public TYsonMessage
{
    std::string Key;
    std::string Payload;

    REGISTER_YSON_STRUCT(TKeyMessage);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("key", &TThis::Key)
            .Default();
        registrar.Parameter("payload", &TThis::Payload)
            .Default();
    }
};

//! Output produced on each visit: one row per key per pass, carrying the stored payload and a
//! monotonically increasing visit_index so the check can tell visits apart.
struct TVisitMessage
    : public TYsonMessage
{
    std::string Key;
    std::string Payload;
    i64 VisitIndex = 0;

    REGISTER_YSON_STRUCT(TVisitMessage);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("key", &TThis::Key)
            .Default();
        registrar.Parameter("payload", &TThis::Payload)
            .Default();
        registrar.Parameter("visit_index", &TThis::VisitIndex)
            .Default(0);
    }
};

//! Per-key user state of TVisitTesterFunction.
struct TUserState
    : public NYTree::TYsonStruct
{
    std::string Payload;
    i64 VisitIndex = 0;

    REGISTER_YSON_STRUCT(TUserState);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("payload", &TThis::Payload)
            .Default();
        registrar.Parameter("visit_index", &TThis::VisitIndex)
            .Default(0);
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Stores each key's payload in internal per-key state on a message, and on a visit emits a
//! TVisitMessage with the stored payload and an incremented visit index.
class TVisitTesterFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        initContext->InitClient<TUserState>(StateClient_, "user_state");
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& context) override
    {
        auto ysonMessage = context->ConvertToYsonMessage<TKeyMessage>(message);
        auto state = StateClient_.GetState(message->Key);
        state->Payload = ysonMessage->Payload;
    }

    void ProcessVisit(
        const TInputVisitConstPtr& visit,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto state = StateClient_.GetState(visit->Key);
        if (state.IsEmpty()) {
            return;
        }
        auto ysonKey = context->ConvertToYsonKey<TKeyMessage>(visit->Key);

        auto outputMessage = New<TVisitMessage>();
        outputMessage->Key = ysonKey->Key;
        outputMessage->Payload = state->Payload;
        outputMessage->VisitIndex = ++state->VisitIndex;

        output->AddMessage(context->ConvertToMessage(outputMessage));
    }

private:
    TMutableStateKeyClient<TUserState> StateClient_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDemo

using namespace NYT::NFlow;
using namespace NYT::NFlow::NDemo;

YT_FLOW_DEFINE_YSON_MESSAGE(TKeyMessage);
YT_FLOW_DEFINE_YSON_MESSAGE(TVisitMessage);

YT_FLOW_DEFINE_PROCESS_FUNCTION(TVisitTesterFunction);

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    TSimpleSpecBuilder builder;
    builder.RegisterStream<TKeyMessage>("keys");
    builder.RegisterStream<TVisitMessage>("visits");
    return TSimpleRunnerProgram(std::move(builder)).Run(argc, argv);
}
