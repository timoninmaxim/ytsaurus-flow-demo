"""Python companion for the word_count_sync scenario: the reader and the counter from
companion/main.cpp re-expressed with the Flow Python companion SDK.

Both computations are registered here; the pipeline binary is the stock flow_server. The
behaviour mirrors the C++ functions exactly:

- ``TextRead`` (hosted by ``TSwiftOrderedSourceCompanionComputation``) splits each input
  text line into whitespace-separated words and emits one message per word. It is a swift
  source, so it must be deterministic — ``str.split()`` is.
- ``WordCount`` (hosted by ``TTransformCompanionComputation``) drops stop words, routes
  words shorter than ``min_word_length`` into the "skipped" stream (written by the sync
  sink inside the epoch transaction), and counts the rest in the external state "/state"
  backed by the word_counts table.

One difference against the C++ variant is structural: the stop words arrive through the
computation's spec ``parameters`` (read via ``ctx.parameters``), not through a
companion-hosted resource — the Python SDK registers computations only, it cannot host a
resource class the way the C++ ``TPipeline::AddResource`` does.
"""

import logging

from yt.yt.flow.library.python.companion import Pipeline, RowFunction

logging.basicConfig(level=logging.INFO)


def _to_str(value):
    return value.decode("utf-8") if isinstance(value, bytes) else value


class TextRead(RowFunction):
    def on_message(self, message, output, ctx):
        for word in message.payload["text"].split():
            builder = ctx.message_builder("words")
            builder.set("word", word)
            output.add_message(builder.finish())


class WordCount(RowFunction):
    def on_message(self, message, output, ctx):
        word = message.payload["word"]

        stop_words = {_to_str(w) for w in ctx.parameters.get("stop_words", [])}
        if word in stop_words:
            return

        if len(word) < ctx.parameters.get("min_word_length", 0):
            builder = ctx.message_builder("skipped")
            builder.set("word", word)
            builder.set("length", len(word))
            output.add_message(builder.finish())
            return

        state = ctx.external_state("/state", message)
        count = state.get("count") or 0
        # Only set() persists external state; mutating the read value would be lost.
        state.set(state.to_builder().set("count", count + 1).finish())


def main():
    pipeline = Pipeline()
    pipeline.add("reader", TextRead(), source=True)
    pipeline.add("counter", WordCount())
    pipeline.run()


if __name__ == "__main__":
    main()
