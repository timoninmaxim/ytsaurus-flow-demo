PROGRAM(word_count_sync_companion)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/companion/server
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/resources
)

END()
