#!/usr/bin/python3
"""The companion_python pipeline program: launcher on the host, companion in the job.

`app.run()` has two modes. Started by hand as `python3 main.py --config pipeline.yson`, it is the
SDK launcher: it ships this file to the worker as the companion, points the CompanionManager
resource at it and execs flow_server ($YT_FLOW_BIN) with the enriched spec. Spawned by the worker (YT_FLOW_COMPANION_CONFIG
set), it serves the registered computations over gRPC.

The "mapper" computation mirrors every typed input column to the output stream (string, int64,
double, boolean -- the companion wire-protocol type roundtrip) and adds one column computed in
Python, so the output visibly proves the row went through this process.

The "reader" computation is native C++ (see pipeline.yson.j2) and is therefore not
registered here: native computations never call the companion.
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


app = Pipeline()
app.add("mapper", map_row)

if __name__ == "__main__":
    app.run()
