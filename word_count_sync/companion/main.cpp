#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>

#include <yt/yt/flow/library/cpp/companion/server/companion_main.h>
#include <yt/yt/flow/library/cpp/companion/server/pipeline.h>

#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>

#include <yt/yt/flow/library/cpp/resources/resource_base.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <util/string/split.h>

#include <algorithm>

namespace NYT::NFlow::NDemo {

////////////////////////////////////////////////////////////////////////////////

//! Parameters of TWordCountFunction.
struct TWordCountParameters
    : public NYTree::TYsonStruct
{
    //! Words shorter than this are not counted; they go into the "skipped" stream instead.
    i64 MinWordLength = 0;

    REGISTER_YSON_STRUCT(TWordCountParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("min_word_length", &TThis::MinWordLength)
            .Default(0);
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Parameters of TStopWordsResource.
struct TStopWordsParameters
    : public NYTree::TYsonStruct
{
    std::vector<std::string> StopWords;

    REGISTER_YSON_STRUCT(TStopWordsParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("stop_words", &TThis::StopWords)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

//! A resource carrying the words to ignore entirely. It is hosted by this companion
//! process: the spec declares it as NCompanion::TCompanionResource and names this class
//! in "companion_resource_class"; the worker drives its lifecycle over gRPC.
class TStopWordsResource
    : public TResourceBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TStopWordsParameters);

    using TResourceBase::TResourceBase;

    bool IsStopWord(const std::string& word) const
    {
        const auto& words = GetParameters()->StopWords;
        return std::find(words.begin(), words.end(), word) != words.end();
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Splits each input text message into words and emits one message per word.
class TTextReadFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto text = GetColumnValue<std::string>(message, "text");
        for (const auto& word : StringSplitter(text).SplitBySet(" \t\n\r").SkipEmpty()) {
            auto builder = context->MakeOutputMessageBuilder();
            builder.Payload().Set<std::string>(std::string(word), "word");
            output->AddMessage(builder.Finish());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Counts word occurrences in external state. Words from the stop-words resource are
//! dropped entirely; of the rest, words shorter than the configured length are skipped
//! and emitted into the "skipped" stream, whose sink writes them into the skipped-words
//! table inside the same epoch transaction that commits the counts.
class TWordCountFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        MinWordLength_ = initContext->GetParameters<TWordCountParameters>()->MinWordLength;
        StopWords_ = initContext->GetStaticResource("StopWords")->As<TStopWordsResource>();
        initContext->InitExternalStateClient(StateClient_, "/state");
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto word = GetColumnValue<std::string>(message, "word");
        if (StopWords_->IsStopWord(word)) {
            return;
        }
        if (std::ssize(word) < MinWordLength_) {
            auto builder = context->MakeOutputMessageBuilder("skipped");
            builder.Payload().Set<std::string>(word, "word");
            builder.Payload().Set<i64>(std::ssize(word), "length");
            output->AddMessage(builder.Finish());
            return;
        }

        auto state = StateClient_.GetState(message->Key);
        auto count = state->GetColumnValue<std::optional<i64>>("count").value_or(0);
        TPayloadBuilder builder(state->Schema);
        builder.Set(count + 1, "count");
        state->Payload = builder.Finish();
    }

private:
    i64 MinWordLength_ = 0;
    TIntrusivePtr<TStopWordsResource> StopWords_;
    TMutableStateKeyClient<TSimpleExternalState> StateClient_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDemo

int main(int argc, const char** argv)
{
    NYT::NFlow::NCompanionServer::TPipeline pipeline;
    pipeline.AddSource<NYT::NFlow::NDemo::TTextReadFunction>("reader");
    pipeline.AddTransform<NYT::NFlow::NDemo::TWordCountFunction, NYT::NFlow::NDemo::TWordCountParameters>("counter");
    pipeline.AddResource<NYT::NFlow::NDemo::TStopWordsResource>();
    return NYT::NFlow::NCompanionServer::RunCompanionMain(argc, argv, std::move(pipeline));
}
