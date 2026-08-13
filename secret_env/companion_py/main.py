"""Python companion for the secret_env scenario: the C++ TSecretChecker's subject re-tested one
process further out.

The C++ variant asserts that the secret from the operation's secure vault is visible inside the
flow job process. This variant asks the next question: does it also reach a *companion* — the
separate process the worker spawns for out-of-process user code? The worker re-exports every
secure-vault entry as a plain env var at startup (`runner/init.cpp`) and spawns the companion
with a full copy of its own environment (`companion_process_manager.cpp`, `copyEnv = true`), so
the answer should be yes — and this computation proves it empirically.

Instead of crashing on a mismatch like the C++ checker, ``SecretChecker`` *reports*: for every
input message it writes what it observed into the output stream — the value of ``YT_MY_SECRET``
in its own environment, and whether the raw ``YT_SECURE_VAULT`` text (also inherited) mentions
the secret's name at all. The verification then compares the reported value against the expected
one from outside, which is stronger evidence than the absence of job failures: the value in the
output queue can only have come from the companion's environment.

The two columns separate the links of the chain exactly like the C++ variant's error message:
``vault_carries_name = true`` with an empty ``secret`` would mean the vault reached the job but
the re-export or the inheritance into the companion broke; both empty would mean the vault never
reached the job at all. (The name check is a plain substring probe of the YSON text, not a parse
— sufficient for a diagnostic column.)
"""

import logging
import os

from yt.yt.flow.library.python.companion import Pipeline, RowFunction

logging.basicConfig(level=logging.INFO)


def _to_str(value):
    return value.decode("utf-8") if isinstance(value, bytes) else value


class SecretChecker(RowFunction):
    def on_message(self, message, output, ctx):
        secret = os.environ.get("YT_MY_SECRET")
        vault = os.environ.get("YT_SECURE_VAULT", "")

        builder = ctx.message_builder("observations")
        builder.set("key", _to_str(message.payload["key"]))
        builder.set("secret", secret if secret is not None else "<unset>")
        builder.set("vault_carries_name", "true" if "YT_MY_SECRET" in vault else "false")
        output.add_message(builder.finish())


def main():
    pipeline = Pipeline()
    pipeline.add("checker", SecretChecker())
    pipeline.run()


if __name__ == "__main__":
    main()
