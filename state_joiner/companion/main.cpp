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

namespace NYT::NFlow::NDemo {

////////////////////////////////////////////////////////////////////////////////

//! Sums each user's "Amount" into the per-user state under "/user_total" and forwards the user id
//! (with a constant bucket) into the "users" stream.
class TAccumulatorFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        initContext->InitExternalStateClient(TotalClient_, "/user_total");
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto state = TotalClient_.GetState(message->Key);
        auto total = state->GetColumnValue<std::optional<i64>>("Total").value_or(0);
        TPayloadBuilder stateBuilder(state->Schema);
        stateBuilder.Set(total + GetColumnValue<i64>(message, "Amount"), "Total");
        state->Payload = stateBuilder.Finish();

        auto builder = context->MakeOutputMessageBuilder();
        builder.Payload().Set<std::string>(GetColumnValue<std::string>(message, "UserId"), "UserId");
        builder.Payload().Set<ui64>(0, "Bucket");
        output->AddMessage(builder.Finish());
    }

private:
    TMutableStateKeyClient<TSimpleExternalState> TotalClient_;
};

////////////////////////////////////////////////////////////////////////////////

//! Joins the state TAccumulatorFunction keeps under "/user_total" and emits the stored total for
//! each incoming user into the "results" stream.
class TJoinerFunction
    : public IBatchProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        initContext->InitExternalStateClient(TotalJoiner_, "/user_total");
    }

    void Process(
        const IInputContextPtr& input,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        for (const auto& message : input->GetMessages()) {
            auto state = TotalJoiner_.GetState(message);
            // A key with no row in the joined table arrives as an all-null state, a key the batch
            // carried nothing for as an uninitialized accessor. Report either as -1 instead of
            // throwing: an exception thrown in a companion is retried forever, whereas a sentinel
            // in the output table makes a broken join visible at a glance.
            auto total = state.IsInitialized()
                ? state->GetColumnValue<std::optional<i64>>("Total").value_or(-1)
                : -1;

            auto builder = context->MakeOutputMessageBuilder();
            builder.Payload().Set<std::string>(GetColumnValue<std::string>(message, "UserId"), "UserId");
            builder.Payload().Set<i64>(total, "Total");
            output->AddMessage(builder.Finish());
        }
    }

private:
    TJoinedStateKeyClient<TSimpleExternalState> TotalJoiner_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDemo

int main(int argc, const char** argv)
{
    NYT::NFlow::NCompanionServer::TPipeline pipeline;
    pipeline.AddTransform<NYT::NFlow::NDemo::TAccumulatorFunction>("accumulator");
    pipeline.AddTransform<NYT::NFlow::NDemo::TJoinerFunction>("joiner");
    return NYT::NFlow::NCompanionServer::RunCompanionMain(argc, argv, std::move(pipeline));
}
