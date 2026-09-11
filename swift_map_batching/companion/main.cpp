#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>

#include <yt/yt/flow/library/cpp/companion/server/companion_main.h>
#include <yt/yt/flow/library/cpp/companion/server/pipeline.h>

#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/split.h>

namespace NYT::NFlow::NDemo {

////////////////////////////////////////////////////////////////////////////////

//! Merges the epoch's messages of one key into a single output message carrying their event ids.
//! Hosted by TSwiftMapCompanionComputation: the merged output has as many parents as the key had
//! messages, which the swift map accepts only under allow_batching_with_relaxed_guarantees.
class TBatchFunction
    : public IKeyedBatchProcessFunction
{
public:
    void ProcessKey(
        const IInputContextPtr& input,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        const auto& messages = input->GetMessages();
        if (messages.empty()) {
            return;
        }

        std::vector<i64> eventIds;
        eventIds.reserve(messages.size());
        for (const auto& message : messages) {
            eventIds.push_back(GetColumnValue<i64>(message, "event_id"));
        }

        auto builder = context->MakeOutputMessageBuilder();
        builder.Payload().Set(JoinSeq(",", eventIds), "event_ids");
        output->AddMessage(builder.Finish());
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Explodes a batched message back into one message per event id, tagging each with the size of
//! the batch it came out of — the only place the merging is visible downstream.
class TWriteFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto eventIds = GetColumnValue<std::string>(message, "event_ids");

        std::vector<i64> parsedEventIds;
        for (const auto& token : StringSplitter(eventIds).Split(',').SkipEmpty()) {
            parsedEventIds.push_back(FromString<i64>(token.Token()));
        }

        for (auto eventId : parsedEventIds) {
            auto builder = context->MakeOutputMessageBuilder();
            builder.Payload().Set<i64>(eventId, "event_id");
            builder.Payload().Set<i64>(std::ssize(parsedEventIds), "batch_size");
            output->AddMessage(builder.Finish());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDemo

int main(int argc, const char** argv)
{
    NYT::NFlow::NCompanionServer::TPipeline pipeline;
    // There is no AddSwiftMap: a swift map is declared with AddTransform and the host class in
    // the spec (TSwiftMapCompanionComputation) is what makes its output unmaterialized.
    pipeline.AddTransform<NYT::NFlow::NDemo::TBatchFunction>("batcher");
    pipeline.AddTransform<NYT::NFlow::NDemo::TWriteFunction>("writer");
    return NYT::NFlow::NCompanionServer::RunCompanionMain(argc, argv, std::move(pipeline));
}
