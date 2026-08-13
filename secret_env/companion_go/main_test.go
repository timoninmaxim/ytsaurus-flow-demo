package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/flow"
	"go.ytsaurus.tech/yt/go/flow/flowtest"
)

func newHarness(t *testing.T) *flowtest.Harness {
	return flowtest.New(t, flow.NewRowComputation("checker", &secretChecker{}), flowtest.Options{
		Streams: map[string]flow.Schema{
			"events":       flowtest.Schema("key:string"),
			"observations": flowtest.Schema("key:string", "secret:string", "vault_carries_name:string"),
		},
		KeySchema: flowtest.Schema("hash:uint64", "key:string"),
	})
}

func TestReportsTheSecretFromItsEnvironment(t *testing.T) {
	t.Setenv("YT_MY_SECRET", "5")
	t.Setenv("YT_SECURE_VAULT", `{"YT_MY_SECRET"="5";"YT_TOKEN"="x";}`)
	h := newHarness(t)

	r := h.Process(h.Message("events", flowtest.Row{"key": "pos-1"}))

	require.Equal(t, []flowtest.Row{
		{"key": "pos-1", "secret": "5", "vault_carries_name": "true"},
	}, r.Rows())
}

func TestReportsAWrongValueVerbatim(t *testing.T) {
	// The wrong-value negative control's shape: the vault delivered the name, the value
	// tracks the launcher verbatim, and verification fails on the value from outside.
	t.Setenv("YT_MY_SECRET", "wrong")
	t.Setenv("YT_SECURE_VAULT", `{"YT_MY_SECRET"="wrong";}`)
	h := newHarness(t)

	r := h.Process(h.Message("events", flowtest.Row{"key": "neg-1"}))

	require.Equal(t, []flowtest.Row{
		{"key": "neg-1", "secret": "wrong", "vault_carries_name": "true"},
	}, r.Rows())
}

func TestSeparatesTheLinksOfTheChain(t *testing.T) {
	// No secret and no vault at all: both columns say so — the shape that would mean the
	// vault never reached the job. (Live this cannot happen: with the variable unset the
	// runner refuses to launch.)
	t.Setenv("YT_MY_SECRET", "")
	t.Setenv("YT_SECURE_VAULT", "")
	// t.Setenv cannot unset, so clear explicitly; it still restores the originals after.
	require.NoError(t, os.Unsetenv("YT_MY_SECRET"))
	require.NoError(t, os.Unsetenv("YT_SECURE_VAULT"))
	h := newHarness(t)

	r := h.Process(h.Message("events", flowtest.Row{"key": "neg-2"}))

	require.Equal(t, []flowtest.Row{
		{"key": "neg-2", "secret": "<unset>", "vault_carries_name": "false"},
	}, r.Rows())
}
