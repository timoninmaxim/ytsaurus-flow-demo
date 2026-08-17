"""Python companion for the companion_python scenario.

The "mapper" computation mirrors every typed input column to the output stream
(string, int64, double, boolean -- the companion wire-protocol type roundtrip)
and adds one column computed in Python, so the output visibly proves the row
went through this process.

The "reader" computation is native C++ (see pipeline.yson.template) and is
therefore not registered here: native computations never call the companion.
"""

import logging

from yt.yt.flow.library.python.companion import Pipeline

logging.basicConfig(level=logging.INFO)

MIRRORED_COLUMNS = ("key", "text", "count", "score", "flag")


def map_row(message, output, ctx):
    out = ctx.message_builder("mapped")
    for column in MIRRORED_COLUMNS:
        out.set(column, message.payload[column])
    text = message.payload["text"]
    out.set("text_upper", text.upper())
    output.add_message(out.finish())


def main():
    pipeline = Pipeline()
    pipeline.add("mapper", map_row)
    pipeline.run()


if __name__ == "__main__":
    main()
