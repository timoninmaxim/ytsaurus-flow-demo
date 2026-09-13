// A pipeline binary of your own: the runner, the random connector, the queue connector and one
// user process function.
//
// TReducer is a transform-mode process function (hosted by the stock
// NYT::NFlow::TProcessFunctionComputation): for every message it loads the per-key state from the
// pipeline's built-in `states` table, bumps a counter, remembers the last payload, and re-emits
// the message into the output stream. The output stream is materialized into `output_messages`
// per epoch and written to a YT queue by the async queue sink (see pipeline.yson.template), so a
// single message loads the whole transform write path: `compact_input_messages`,
// `output_messages`, `states`, and the output queue.

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

namespace NYT::NFlow::NDemo {

////////////////////////////////////////////////////////////////////////////////

// Per-key state stored in the built-in `states` table.
struct TReducerState
    : public NYTree::TYsonStruct
{
    i64 Count{};
    std::string LastData;

    REGISTER_YSON_STRUCT(TReducerState);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("count", &TThis::Count)
            .Default();
        registrar.Parameter("last_data", &TThis::LastData)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TReducer
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        initContext->InitClient<TReducerState>(StateClient_, "state");
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto state = StateClient_.GetState(message->Key);
        state->Count += 1;
        state->LastData = GetColumnValue<std::string>(message, "data");

        const auto& streamId = *context->GetSpec()->OutputStreamIds.begin();
        auto builder = context->MakeOutputMessageBuilder(streamId);
        builder.Payload().Set<TStringBuf>(GetColumnValue<TStringBuf>(message, "key"), "key");
        builder.Payload().Set<TStringBuf>(GetColumnValue<TStringBuf>(message, "data"), "data");
        output->AddMessage(builder.Finish());
    }

private:
    TMutableStateKeyClient<TReducerState> StateClient_;
};

YT_FLOW_DEFINE_PROCESS_FUNCTION(TReducer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDemo

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
