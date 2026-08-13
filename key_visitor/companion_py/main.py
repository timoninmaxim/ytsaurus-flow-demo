"""Python companion for the key_visitor scenario: the visit tester from pipeline/main.cpp
re-expressed with the Flow Python companion SDK.

Only the "tester" computation is registered here — the native "key_reader" runs in-process
in the stock flow_server worker and never calls the companion. The behaviour mirrors
NYT::NFlow::NDemo::TVisitTesterFunction exactly:

- on a message, store its payload in the per-key internal state "user_state"
  (declared in the spec's ``parameters/internal_states``);
- on a visit, emit the *stored* payload together with a per-key visit counter,
  or nothing if the key has no state yet.

The engine drives everything else (key tracking, the periodic sweep, the final pass after
the finite source drains) identically to the C++ variant: visits are injected into this
process through the companion protocol, together with the states of the visited keys.
"""

import logging

from yt.yt.flow.library.python.companion import Pipeline, RowFunction

logging.basicConfig(level=logging.INFO)

STATE_NAME = "user_state"


class VisitTester(RowFunction):
    def on_message(self, message, output, ctx):
        state = ctx.state(STATE_NAME, message)
        # Preserve visit_index across payload updates; only set() persists the change.
        data = state.get_or_default({"payload": "", "visit_index": 0})
        data["payload"] = message.payload["payload"]
        state.set(data)

    def on_visit(self, visit, output, ctx):
        state = ctx.state(STATE_NAME, visit)
        data = state.get()
        if data is None:
            return

        data["visit_index"] += 1

        out = ctx.message_builder("visits")
        out.set("key", visit.key["key"])
        out.set("payload", data["payload"])
        out.set("visit_index", data["visit_index"])
        output.add_message(out.finish())

        state.set(data)


def main():
    pipeline = Pipeline()
    pipeline.add("tester", VisitTester())
    pipeline.run()


if __name__ == "__main__":
    main()
