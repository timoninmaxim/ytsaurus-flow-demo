// Go companion for the secret_env scenario: the Python variant's SecretChecker re-expressed
// with the Flow Go SDK — the same subject, one more process shape.
//
// The C++ variant asserts that the secret from the operation's secure vault is visible inside
// the flow job process; the Python variant proved it also reaches a companion child. This
// variant re-asks both questions for the Go shape, which adds a wrinkle on the LAUNCHER side:
// the pipeline binary is its own runner — pipeline.Run() enriches the spec and then *execs*
// flow_server (`yt/go/flow/runner/runner.go`, syscall.Exec with os.Environ()), so the secret
// exported in the shell must survive that exec before the usual chain even starts:
//
//  1. exec passes the launcher's full environment to flow_server;
//  2. flow_server's vanilla launcher reads each name declared in the spec's `secret_env`
//     from that environment into the operation's secure vault
//     (`library/cpp/vanilla/spec.cpp`, InjectSecureVaultFromEnv);
//  3. inside the job, YT delivers the vault as YT_SECURE_VAULT and Flow re-exports each entry
//     as a plain env var (`library/cpp/runner/init.cpp`, Initialize);
//  4. the worker spawns this binary again — now as the companion — with a full copy of its
//     own environment (`library/cpp/companion/companion_process_manager.cpp`, copyEnv=true).
//
// Like the Python checker, secretChecker *reports* instead of crashing: for every input
// message it writes what it observed into the output stream — the value of YT_MY_SECRET in
// its own environment, and whether the inherited raw YT_SECURE_VAULT text mentions the name
// at all (a substring probe, diagnostic only). The verification then compares the reported
// value from outside; the value in the output queue can only have come from this process's
// environment. The two columns separate the links of the chain: vault_carries_name = "true"
// with an empty secret would mean the vault reached the job but the re-export or the
// inheritance broke; both empty would mean the vault never reached the job at all.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"go.ytsaurus.tech/yt/go/flow"
)

// secretEnvName is the env var the spec's `secret_env` line routes through the secure vault.
const secretEnvName = "YT_MY_SECRET"

// eventMessage is an input on the "events" stream: an opaque key from the prepared queue.
type eventMessage struct {
	flow.YSONMessage
	Key string `yson:"key"`
}

// observationMessage is an output on the "observations" stream: what the companion saw in its
// own environment when it processed the keyed message.
type observationMessage struct {
	flow.YSONMessage
	Key              string `yson:"key"`
	Secret           string `yson:"secret"`
	VaultCarriesName string `yson:"vault_carries_name"`
}

// secretChecker reports the secret as observed in the companion process's environment.
type secretChecker struct{}

var _ flow.RowFunction = (*secretChecker)(nil)

func (*secretChecker) OnMessage(
	ctx context.Context,
	rt flow.Runtime,
	msg flow.ExtendedMessage,
	out flow.OutputCollector,
) error {
	var input eventMessage
	if err := msg.ConvertTo(&input); err != nil {
		return err
	}

	secret, ok := os.LookupEnv(secretEnvName)
	if !ok {
		secret = "<unset>"
	}
	vault := os.Getenv("YT_SECURE_VAULT")

	output := flow.NewYSONMessage[observationMessage]("observations")
	output.Key = input.Key
	output.Secret = secret
	output.VaultCarriesName = fmt.Sprintf("%t", strings.Contains(vault, secretEnvName))

	message, err := flow.ConvertFrom(rt, output)
	if err != nil {
		return err
	}
	out.AddMessage(message)
	return nil
}

func main() {
	pipeline := flow.NewPipeline()
	pipeline.AddStreams(
		flow.NewYSONStream[eventMessage]("events"),
		flow.NewYSONStream[observationMessage]("observations"),
	)
	pipeline.Add(flow.NewRowComputation("checker", &secretChecker{}))

	if err := pipeline.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "secret_env_go: %v\n", err)
		os.Exit(1)
	}
}
