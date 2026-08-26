package tech.ytsaurus.flow.demo.wordcountsync;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.computation.SourceComputation;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Message;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.flow.testutils.TestComputationHarness;
import tech.ytsaurus.flow.testutils.TestDoProcessRequest;
import tech.ytsaurus.flow.testutils.TestDoProcessResponse;
import tech.ytsaurus.typeinfo.TiType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Offline tests of the word logic, mirroring the Go variant's {@code main_test.go}: both
 * computations are driven through {@link TestComputationHarness} — no cluster needed. The
 * engine-side subject (the queue source, the epoch transaction, the sync sink) is proven by
 * the live run.
 */
public class WordCountSyncTest {

    private static final TableSchema LINES_SCHEMA = TableSchema.builder()
            .addValue("text", TiType.string())
            .build();

    private static final TableSchema WORDS_SCHEMA = TableSchema.builder()
            .addValue("word", TiType.string())
            .build();

    private static final TableSchema KEY_SCHEMA = TableSchema.builder()
            .addValue("hash", TiType.uint64())
            .addValue("word", TiType.string())
            .build();

    private static final TableSchema READER_KEY_SCHEMA = TableSchema.builder()
            .addValue("hash", TiType.uint64())
            .addValue("text", TiType.string())
            .build();

    // The word_counts table backing the external state "/state": keyed exactly by the
    // counter's group_by_schema, with the count as the single value column.
    private static final TableSchema STATE_SCHEMA = TableSchema.builder()
            .addValue("hash", TiType.uint64())
            .addValue("word", TiType.string())
            .addValue("count", TiType.int64())
            .build();

    private TestComputationHarness harness;

    @BeforeEach
    public void setUp() {
        var context = new PipelineContext();
        context.registerComputation(SourceComputation.builder()
                .setComputationId("reader")
                .setProcessFunction(new TextRead())
                .build());
        context.registerComputation(Computation.builder()
                .setComputationId("counter")
                .setProcessFunction(new WordCount())
                .build());
        harness = TestComputationHarness.builder()
                .setPipelineContext(context)
                .setPipelineSpec(getClass().getClassLoader().getResourceAsStream("pipeline.yson"))
                .addExternalStateSchema("/state", STATE_SCHEMA)
                .build();
    }

    private static Payload keyOf(String word) {
        // Any deterministic per-word hash works offline; live, the engine computes farm_hash.
        return new PayloadBuilder(KEY_SCHEMA)
                .set("hash", Integer.toUnsignedLong(word.hashCode()))
                .set("word", word)
                .finish();
    }

    private static ExtendedMessage lineMessage(String text) {
        return ExtendedMessage.builder()
                .setMessageId(GUID.create().toString())
                .setStreamId("lines")
                .setKey(new PayloadBuilder(READER_KEY_SCHEMA)
                        .set("hash", 0L)
                        .set("text", text)
                        .finish())
                .setPayload(new PayloadBuilder(LINES_SCHEMA).set("text", text).finish())
                .build();
    }

    private static ExtendedMessage wordMessage(String word) {
        return ExtendedMessage.builder()
                .setMessageId(GUID.create().toString())
                .setStreamId("words")
                .setKey(keyOf(word))
                .setPayload(new PayloadBuilder(WORDS_SCHEMA).set("word", word).finish())
                .build();
    }

    private Long countOf(TestDoProcessResponse response, String word) {
        return response.allStates().get(WordCount.COUNT_STATE, keyOf(word)).get()
                .map(payload -> payload.get("count", Long.class))
                .orElse(null);
    }

    @Test
    public void testReaderSplitsInOrder() {
        var response = harness.doProcess(TestDoProcessRequest.builder("reader")
                .setMessages(List.of(lineMessage("hello to a world")))
                .build());

        var words = response.getOutputMessagesFlatten().stream()
                .map(message -> message.get("word", String.class))
                .toList();
        assertEquals(List.of("hello", "to", "a", "world"), words);
        assertTrue(response.getOutputMessagesFlatten().stream()
                .allMatch(message -> message.getStreamId().equals("words")));
    }

    @Test
    public void testStopWordsAreDroppedEntirely() {
        // "flow" is four letters long, so only the stop_words parameter can drop it; "to" is a
        // stop word before it is a short word.
        var response = harness.doProcess(TestDoProcessRequest.builder("counter")
                .setMessages(List.of(wordMessage("flow"), wordMessage("to")))
                .build());

        assertTrue(response.getOutputMessagesFlatten().isEmpty());
        assertTrue(response.modifiedStates().externalNames().isEmpty());
    }

    @Test
    public void testShortWordsAreSkippedWithLength() {
        var response = harness.doProcess(TestDoProcessRequest.builder("counter")
                .setMessages(List.of(wordMessage("is")))
                .build());

        var skipped = response.getOutputMessagesFlatten();
        assertEquals(1, skipped.size());
        assertEquals("skipped", skipped.get(0).getStreamId());
        assertEquals("is", skipped.get(0).get("word", String.class));
        assertEquals(2L, skipped.get(0).get("length", Long.class));
        assertTrue(response.modifiedStates().externalNames().isEmpty());
    }

    @Test
    public void testCountingStartsAtOne() {
        var response = harness.doProcess(TestDoProcessRequest.builder("counter")
                .setMessages(List.of(wordMessage("hello")))
                .build());

        assertTrue(response.getOutputMessagesFlatten().isEmpty());
        assertEquals(1L, countOf(response, "hello"));
    }

    @Test
    public void testCountingAddsToPriorState() {
        var response = harness.doProcess(TestDoProcessRequest.builder("counter")
                .setMessages(List.of(wordMessage("hello")))
                .setState(WordCount.COUNT_STATE, keyOf("hello"),
                        new PayloadBuilder(STATE_SCHEMA)
                                .set("hash", Integer.toUnsignedLong("hello".hashCode()))
                                .set("word", "hello")
                                .set("count", 2L)
                                .finish())
                .build());

        assertEquals(3L, countOf(response, "hello"));
    }

    @Test
    public void testCountingToleratesNullCount() {
        // Live, a key with no count yet can arrive as a present state row with the key columns
        // set and "count" null (see the Go variant's README); the counter must treat it as zero
        // rather than fail.
        var response = harness.doProcess(TestDoProcessRequest.builder("counter")
                .setMessages(List.of(wordMessage("hello")))
                .setState(WordCount.COUNT_STATE, keyOf("hello"),
                        new PayloadBuilder(STATE_SCHEMA)
                                .set("hash", Integer.toUnsignedLong("hello".hashCode()))
                                .set("word", "hello")
                                .finish())
                .build());

        assertEquals(1L, countOf(response, "hello"));
    }

    @Test
    public void testRepeatedWordInOneBatchCountsTwice() {
        var response = harness.doProcess(TestDoProcessRequest.builder("counter")
                .setMessages(List.of(wordMessage("good"), wordMessage("good")))
                .build());

        assertEquals(2L, countOf(response, "good"));
    }

    @Test
    public void testScenarioEndToEnd() {
        // The scenario's two input lines, split by the reader...
        var readerResponse = harness.doProcess(TestDoProcessRequest.builder("reader")
                .setMessages(List.of(
                        lineMessage("hello to a world"),
                        lineMessage("flow is on it")))
                .build());
        var words = new ArrayList<ExtendedMessage>();
        for (Message message : readerResponse.getOutputMessagesFlatten()) {
            words.add(wordMessage(message.get("word", String.class)));
        }

        // ...then counted in one batch.
        var counterResponse = harness.doProcess(TestDoProcessRequest.builder("counter")
                .setMessages(words)
                .build());

        // word_counts: exactly the two counted words.
        assertEquals(1L, countOf(counterResponse, "hello"));
        assertEquals(1L, countOf(counterResponse, "world"));
        assertNull(countOf(counterResponse, "flow"));
        assertNull(countOf(counterResponse, "to"));

        // skipped_words: the four short words with their lengths.
        Map<String, Long> skipped = new LinkedHashMap<>();
        for (Message message : counterResponse.getOutputMessagesFlatten()) {
            assertEquals("skipped", message.getStreamId());
            skipped.put(
                    message.get("word", String.class),
                    message.get("length", Long.class));
        }
        assertEquals(Map.of("a", 1L, "is", 2L, "it", 2L, "on", 2L), skipped);
    }
}
