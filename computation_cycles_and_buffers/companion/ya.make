PROGRAM(computation_cycles_companion)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/companion/server
    yt/yt/flow/library/cpp/computation
)

END()
