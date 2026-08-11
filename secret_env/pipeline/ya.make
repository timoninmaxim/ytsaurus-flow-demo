PROGRAM(secret_env_pipeline)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/connectors/random
    yt/yt/flow/library/cpp/runner
)

END()
