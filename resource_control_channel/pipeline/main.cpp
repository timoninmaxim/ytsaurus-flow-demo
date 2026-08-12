// A pipeline binary of your own: the runner, the random connector and the counter resource.
//
// The scenario's subject is the engine's resource control channel: a custom resource is hosted on
// the controller and on every worker, the controller side keeps publishing new target-revision
// spec payloads, and the flow view proves that every instance decoded the delivered payload.
//
// TCounterResourceController runs inside the pipeline controller: every generation_period it bumps
// a counter and publishes it as the target-revision spec. TCounterResource runs on each worker
// (and once on the controller, since the spec requests a controller-side instance); it reports, as
// its applied revision id, the value it decodes from the *delivered payload* — so the ids the
// controller aggregates into its flow view prove the payload content crossed the wire, not just a
// revision stamp. TNullFunction discards the random-source messages; the data path exists only to
// keep workers busy while the resource is exercised.

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/resource.h>
#include <yt/yt/flow/library/cpp/common/resource_controller.h>

#include <yt/yt/flow/library/cpp/resources/resource_base.h>
#include <yt/yt/flow/library/cpp/resources/resource_controller_base.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NFlow::NDemo {

using namespace NYT::NYTree;

////////////////////////////////////////////////////////////////////////////////

//! Discards the random-source messages; the pipeline exists only to keep workers busy
//! while the counter resource is exercised.
class TNullFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& /*message*/,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    { }
};

YT_FLOW_DEFINE_PROCESS_FUNCTION(TNullFunction);

////////////////////////////////////////////////////////////////////////////////

struct TCounterParameters
    : public virtual TYsonStruct
{
    TDuration GenerationPeriod;

    REGISTER_YSON_STRUCT(TCounterParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("generation_period", &TThis::GenerationPeriod)
            .Default(TDuration::Seconds(1));
    }
};

//! Controller side of the counter: bumps a number every generation_period and publishes it
//! as the target revision spec; reflects the per-worker applied revisions into the flow view.
class TCounterResourceController
    : public TResourceControllerBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TCounterParameters);

    using TResourceControllerBase::TResourceControllerBase;

    INodePtr BuildTargetRevisionSpec() override
    {
        auto now = TInstant::Now();
        if (Value_ == 0 || now - LastBumpTime_ >= GetParameters()->GenerationPeriod) {
            ++Value_;
            LastBumpTime_ = now;
        }
        // clang-format off
        return BuildYsonNodeFluently()
            .BeginMap()
                .Item("value").Value(Value_)
            .EndMap();
        // clang-format on
    }

    void CollectStatuses(
        const THashMap<std::string, TWorkerResourceStatusPtr>& workerStatuses,
        const TWorkerResourceStatusPtr& controllerStatus) override
    {
        WorkerStatuses_ = workerStatuses;
        ControllerStatus_ = controllerStatus;
    }

    IMapNodePtr GetView() override
    {
        THashMap<i64, int> workersPerValue;
        for (const auto& [workerAddress, status] : WorkerStatuses_) {
            if (status && status->AppliedRevisionId) {
                ++workersPerValue[*status->AppliedRevisionId];
            }
        }
        // clang-format off
        return BuildYsonNodeFluently()
            .BeginMap()
                .Item("value").Value(Value_)
                .Item("worker_count").Value(std::ssize(WorkerStatuses_))
                // The applied id reported by each worker is the value it decoded from the
                // delivered spec payload (see TCounterResource), so this histogram proves the
                // payload -- not just the revision id -- crossed the wire.
                .Item("workers_per_value").DoMapFor(
                    workersPerValue,
                    [] (auto fluent, const auto& pair) {
                        fluent.Item(ToString(pair.first)).Value(pair.second);
                    })
                .DoIf(ControllerStatus_ && ControllerStatus_->AppliedRevisionId.has_value(), [&] (auto fluent) {
                    fluent.Item("controller_value").Value(*ControllerStatus_->AppliedRevisionId);
                })
            .EndMap()
            ->AsMap();
        // clang-format on
    }

private:
    i64 Value_ = 0;
    TInstant LastBumpTime_;
    THashMap<std::string, TWorkerResourceStatusPtr> WorkerStatuses_;
    TWorkerResourceStatusPtr ControllerStatus_;
};

//! Worker side of the counter. It reports, as its applied revision, the value it decodes from
//! the delivered spec payload -- so the reported id reflects the payload content that crossed
//! the wire, not the framework's revision stamp.
class TCounterResource
    : public TResourceBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TCounterParameters);

    using TController = TCounterResourceController;

    using TResourceBase::TResourceBase;

    TResourceRevisionState GetRevisionState() const override
    {
        auto dynamicContext = GetDynamicContext();
        if (!dynamicContext->TargetRevision || !dynamicContext->TargetRevision->Spec) {
            return {};
        }
        auto value = dynamicContext->TargetRevision->Spec->AsMap()->GetChildValueOrThrow<i64>("value");
        return {
            .AppliedRevisionId = value,
            .TargetRevisionId = value,
        };
    }
};

YT_FLOW_DEFINE_RESOURCE(TCounterResource);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDemo

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
