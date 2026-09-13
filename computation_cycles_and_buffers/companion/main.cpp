#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>

#include <yt/yt/flow/library/cpp/companion/server/companion_main.h>
#include <yt/yt/flow/library/cpp/companion/server/pipeline.h>

#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow::NDemo {

////////////////////////////////////////////////////////////////////////////////

//! Parameters of TCyclePassthroughFunction.
struct TCycleParameters
    : public NYTree::TYsonStruct
{
    //! Output stream each input stream is forwarded to. An input stream missing here is an
    //! error: the topology of this scenario is the point, so a message with nowhere to go
    //! must be reported rather than dropped.
    THashMap<TStreamId, TStreamId> PassthroughRules;

    //! Artificial per-message delay, by input stream. Slows the cycle down enough for the
    //! buffers between its computations to fill.
    THashMap<TStreamId, TDuration> SleepPerMessage;

    REGISTER_YSON_STRUCT(TCycleParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("passthrough_rules", &TThis::PassthroughRules)
            .Default();
        registrar.Parameter("sleep_per_message", &TThis::SleepPerMessage)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Reads the input queue and republishes the "data" column.
class TReadFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto builder = context->MakeOutputMessageBuilder();
        builder.Payload().SetValue(GetColumn(message, "data"), "data");
        output->AddMessage(builder.Finish());
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Forwards every message to the output stream its input stream maps to. All four
//! computations of the cycle are this one function under different parameters; which of them
//! is a transform and which is a swift map is decided by the host class in the spec.
class TCyclePassthroughFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        Parameters_ = initContext->GetParameters<TCycleParameters>();
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        NConcurrency::TDelayedExecutor::WaitForDuration(
            Parameters_->SleepPerMessage.Value(message->StreamId, TDuration::Zero()));

        auto ruleIt = Parameters_->PassthroughRules.find(message->StreamId);
        THROW_ERROR_EXCEPTION_IF(ruleIt == Parameters_->PassthroughRules.end(),
            "No passthrough rule for input stream %Qv",
            message->StreamId);

        auto builder = context->MakeOutputMessageBuilder(ruleIt->second);
        builder.Payload().SetValue(GetColumn(message, "data"), "data");
        output->AddMessage(builder.Finish());
    }

private:
    TIntrusivePtr<TCycleParameters> Parameters_;
};

////////////////////////////////////////////////////////////////////////////////

//! Counts, per key, how many messages came out of the cycle. The count in the external state
//! table is the scenario's assertion: it must equal the number of input messages exactly.
class TReduceFunction
    : public IKeyedBatchProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        initContext->InitExternalStateClient(StateClient_, "/state");
    }

    void ProcessKey(
        const IInputContextPtr& input,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    {
        if (input->GetMessages().empty()) {
            return;
        }

        auto state = StateClient_.GetState(input->GetMessages()[0]->Key);
        NConcurrency::TDelayedExecutor::WaitForDuration(
            TDuration::MilliSeconds(10) * std::ssize(input->GetMessages()));

        auto count = state->GetColumnValue<std::optional<i64>>("count").value_or(0);
        count += std::ssize(input->GetMessages());

        TPayloadBuilder builder(state->Schema);
        builder.Set(count, "count");
        state->Payload = builder.Finish();
    }

private:
    TMutableStateKeyClient<TSimpleExternalState> StateClient_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDemo

int main(int argc, const char** argv)
{
    NYT::NFlow::NCompanionServer::TPipeline pipeline;
    pipeline.AddSource<NYT::NFlow::NDemo::TReadFunction>("reader");
    // AddTransform declares every non-source computation; there is no separate swift-map
    // declaration, and the kind the companion advertises is not cross-checked against the
    // host class the spec picks. The two swift maps are hosted by
    // TSwiftMapCompanionComputation and the two transforms by TTransformCompanionComputation;
    // here they are the same function under four ids.
    pipeline.AddTransform<NYT::NFlow::NDemo::TCyclePassthroughFunction, NYT::NFlow::NDemo::TCycleParameters>("transform_a");
    pipeline.AddTransform<NYT::NFlow::NDemo::TCyclePassthroughFunction, NYT::NFlow::NDemo::TCycleParameters>("swift_map_a");
    pipeline.AddTransform<NYT::NFlow::NDemo::TCyclePassthroughFunction, NYT::NFlow::NDemo::TCycleParameters>("transform_b");
    pipeline.AddTransform<NYT::NFlow::NDemo::TCyclePassthroughFunction, NYT::NFlow::NDemo::TCycleParameters>("swift_map_b");
    pipeline.AddTransform<NYT::NFlow::NDemo::TReduceFunction>("reducer");
    return NYT::NFlow::NCompanionServer::RunCompanionMain(argc, argv, std::move(pipeline));
}
