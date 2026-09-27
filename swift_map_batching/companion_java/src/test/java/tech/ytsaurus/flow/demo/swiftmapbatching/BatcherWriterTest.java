package tech.ytsaurus.flow.demo.swiftmapbatching;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.computation.TransformResult;
import tech.ytsaurus.flow.job.Job;
import tech.ytsaurus.flow.request.RequestContext;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.stream.FlowStreams;
import tech.ytsaurus.flow.stream.StreamIdsMapping;
import tech.ytsaurus.flow.stream.StreamSpecs;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.ysontree.YTree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Offline tests of the batching logic, mirroring the Go variant's {@code main_test.go}: the
 * per-key grouping, the first-appearance group order and the per-group parent scoping — the
 * determinism obligations of swift hosting. The engine-side subject (the swift meta setter, the
 * merge tracker, the relaxed guarantee) is out of scope here and is proven by the live run.
 *
 * <p>The functions are driven through {@link Computation#doProcess} with hand-built requests
 * rather than through {@code flow-test-utils}' {@code TestComputationHarness}: the harness's
 * request-side proto conversion overwrites every message id with a fixed placeholder
 * ({@code MessageProtoMapper.FAKE_MESSAGE_ID}), so the per-group parent ids — the point of
 * these tests — cannot be asserted through it.
 */
public class BatcherWriterTest {

    private static final TableSchema EVENT_IN_SCHEMA = TableSchema.builder()
            .addValue("event_id", TiType.int64())
            .addValue("group_key", TiType.uint64())
            .build();

    private static final TableSchema EVENT_BATCHED_SCHEMA = TableSchema.builder()
            .addValue("event_ids", TiType.string())
            .build();

    private static final TableSchema SINK_EVENT_SCHEMA = TableSchema.builder()
            .addValue("event_id", TiType.int64())
            .addValue("batch_size", TiType.int64())
            .build();

    private static final TableSchema KEY_SCHEMA = TableSchema.builder()
            .add(ColumnSchema.builder("hash", ColumnValueType.UINT64, true).build())
            .addValue("group_key", TiType.uint64())
            .build();

    private Computation batcher;
    private Computation writer;
    private StreamSpecs streamSpecs;

    @BeforeEach
    public void setUp() {
        batcher = Computation.builder()
                .setComputationId("batcher")
                .setProcessFunction(new Batcher())
                .build();
        writer = Computation.builder()
                .setComputationId("writer")
                .setProcessFunction(new Writer())
                .build();

        var mapping = StreamIdsMapping.builder()
                .addMapping("event_in", 0L)
                .addMapping("event_batched", 1L)
                .addMapping("sink_event", 2L)
                .build();
        streamSpecs = new StreamSpecs(mapping, List.of(
                FlowStreams.raw("event_in", EVENT_IN_SCHEMA),
                FlowStreams.raw("event_batched", EVENT_BATCHED_SCHEMA),
                FlowStreams.raw("sink_event", SINK_EVENT_SCHEMA)));
    }

    private Payload keyOf(long groupKey) {
        return new Payload(
                new UnversionedRow(List.of(
                        new UnversionedValue(0, ColumnValueType.UINT64, false, groupKey),
                        new UnversionedValue(1, ColumnValueType.UINT64, false, groupKey))),
                KEY_SCHEMA);
    }

    private ExtendedMessage eventMessage(String messageId, long eventId, long groupKey) {
        return ExtendedMessage.builder()
                .setMessageId(messageId)
                .setStreamId("event_in")
                .setKey(keyOf(groupKey))
                .setPayload(new Payload(
                        new UnversionedRow(List.of(
                                new UnversionedValue(0, ColumnValueType.INT64, false, eventId),
                                new UnversionedValue(1, ColumnValueType.UINT64, false, groupKey))),
                        EVENT_IN_SCHEMA))
                .build();
    }

    private ExtendedMessage batchedMessage(String messageId, String eventIds) {
        return ExtendedMessage.builder()
                .setMessageId(messageId)
                .setStreamId("event_batched")
                .setKey(Payload.EMPTY)
                .setPayload(new Payload(
                        new UnversionedRow(List.of(
                                new UnversionedValue(0, ColumnValueType.STRING, false, eventIds.getBytes()))),
                        EVENT_BATCHED_SCHEMA))
                .build();
    }

    private List<TransformResult> process(Computation computation, List<ExtendedMessage> messages) throws Exception {
        var job = new Job(
                GUID.create(),
                computation.getComputationId(),
                streamSpecs,
                YTree.mapBuilder().key("parameters").beginMap().endMap().endMap().build(),
                YTree.mapBuilder().key("parameters").beginMap().endMap().endMap().build(),
                KEY_SCHEMA);
        var response = computation.doProcess(RequestContext.builder()
                .setComputationId(computation.getComputationId())
                .setRequestId(GUID.create())
                .setJobId(job.getJobId())
                .setJob(job)
                .setMessages(messages)
                .setStreamSpecsOverride(streamSpecs)
                .build());
        // Only the groups that carry messages: the default whole-batch collector stays unused
        // by the batcher and must not surface as an output group.
        return response.getTransformResults().stream()
                .filter(result -> !result.getMessages().isEmpty())
                .toList();
    }

    private List<ExtendedMessage> mixedKeyBatch() {
        // Keys interleaved on purpose: 7, 3, 7, 9, 3, 7.
        return List.of(
                eventMessage("m0", 0, 7),
                eventMessage("m1", 1, 3),
                eventMessage("m2", 2, 7),
                eventMessage("m3", 3, 9),
                eventMessage("m4", 4, 3),
                eventMessage("m5", 5, 7));
    }

    @Test
    public void batcherMergesPerKeyWithExactParentGroups() throws Exception {
        var groups = process(batcher, mixedKeyBatch());

        // One merged message per key, groups in first-appearance order of the keys.
        assertEquals(3, groups.size());

        assertEquals(List.of("m0", "m2", "m5"), groups.get(0).getParentIds());
        assertEquals(1, groups.get(0).getMessages().size());
        assertEquals("0,2,5", groups.get(0).getMessages().get(0).get("event_ids", String.class));

        assertEquals(List.of("m1", "m4"), groups.get(1).getParentIds());
        assertEquals("1,4", groups.get(1).getMessages().get(0).get("event_ids", String.class));

        assertEquals(List.of("m3"), groups.get(2).getParentIds());
        assertEquals("3", groups.get(2).getMessages().get(0).get("event_ids", String.class));

        for (var group : groups) {
            assertEquals("event_batched", group.getMessages().get(0).getStreamId());
        }
    }

    @Test
    public void batcherIsDeterministicAcrossRuns() throws Exception {
        // A swift replay must reproduce the same outputs with the same parent sequences in the
        // same order; re-processing the identical batch pins that offline.
        var first = process(batcher, mixedKeyBatch());
        var second = process(batcher, mixedKeyBatch());

        assertEquals(first.size(), second.size());
        for (int i = 0; i < first.size(); i++) {
            assertEquals(first.get(i).getParentIds(), second.get(i).getParentIds());
            assertEquals(
                    first.get(i).getMessages().get(0).get("event_ids", String.class),
                    second.get(i).getMessages().get(0).get("event_ids", String.class));
        }
    }

    @Test
    public void singleKeyBatchYieldsOneMergedMessage() throws Exception {
        var groups = process(batcher, List.of(
                eventMessage("m0", 100, 4),
                eventMessage("m1", 101, 4),
                eventMessage("m2", 102, 4)));

        assertEquals(1, groups.size());
        assertEquals(List.of("m0", "m1", "m2"), groups.get(0).getParentIds());
        assertEquals("100,101,102", groups.get(0).getMessages().get(0).get("event_ids", String.class));
        assertTrue(groups.get(0).getTimers().isEmpty());
    }

    @Test
    public void writerExplodesBatchTaggingBatchSize() throws Exception {
        var groups = process(writer, List.of(batchedMessage("b0", "10,11,12")));

        assertEquals(1, groups.size());
        assertEquals(List.of("b0"), groups.get(0).getParentIds());

        var rows = groups.get(0).getMessages();
        assertEquals(3, rows.size());
        for (int i = 0; i < rows.size(); i++) {
            assertEquals("sink_event", rows.get(i).getStreamId());
            assertEquals(10L + i, rows.get(i).get("event_id", Long.class));
            assertEquals(3L, rows.get(i).get("batch_size", Long.class));
        }
    }
}
