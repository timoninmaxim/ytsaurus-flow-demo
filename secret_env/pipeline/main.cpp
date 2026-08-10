// A pipeline binary of your own: a minimal Flow runner binary — the runner, the random connector,
// and one user computation. It is not the stock flow_server, which links more connectors.
//
// TSecretChecker asserts that the value declared in the spec's `vanilla/secret_env` is visible
// inside the vanilla job as a plain environment variable. The source is finite, so the pipeline
// reaches Completed only if every message passed the assertion; a missing or wrong secret fails
// the job and the pipeline never completes.

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/generic/algorithm.h>

#include <util/string/join.h>

#include <util/system/env.h>

namespace NYT::NFlow::NDemo {

namespace {

////////////////////////////////////////////////////////////////////////////////

// The value the scenario ships in YT_MY_SECRET; see the README.
constexpr TStringBuf ExpectedSecret = "5";

// Names (never values) of the entries YT delivered in the operation's secure vault. Reported when
// the assertion fails, so the two links of the chain can be told apart: a vault that carries the
// name means the secret reached the job and only the re-export into the environment is missing.
std::vector<std::string> GetSecureVaultKeys()
{
    auto vault = NYTree::ConvertToNode(NYson::TYsonString(GetEnv("YT_SECURE_VAULT", "{}")));
    auto keys = vault->AsMap()->GetKeys();
    Sort(keys);
    return keys;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

class TSecretChecker
    : public TSwiftOrderedSourceComputation
{
public:
    using TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation;

    void DoProcessMessage(const TMessage& /*message*/, IOutputCollectorPtr /*output*/) override
    {
        auto secret = GetEnv("YT_MY_SECRET");
        THROW_ERROR_EXCEPTION_UNLESS(secret == ExpectedSecret,
            "YT_MY_SECRET did not reach the vanilla job as expected (length %v, secure vault carries [%v])",
            std::ssize(secret),
            JoinSeq(", ", GetSecureVaultKeys()));
    }
};

YT_FLOW_DEFINE_COMPUTATION(TSecretChecker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDemo

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
