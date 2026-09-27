package tech.ytsaurus.flow.demo.standalonedirect;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.flow.testutils.TestComputationHarness;
import tech.ytsaurus.flow.testutils.TestDoProcessRequest;
import tech.ytsaurus.typeinfo.TiType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Offline test of the filtering logic, driven through {@link TestComputationHarness} -- no
 * cluster needed. The engine-side subject (the group-by key, the sync sink) is proven by the
 * live run described in the scenario README.
 */
public class MessageFilterTest {

    private static final TableSchema EVENT_SCHEMA = TableSchema.builder()
            .addValue("key", TiType.string())
            .addValue("data", TiType.string())
            .build();

    private static final TableSchema KEY_SCHEMA = TableSchema.builder()
            .addValue("hash", TiType.uint64())
            .addValue("key", TiType.string())
            .build();

    private TestComputationHarness harness;

    @BeforeEach
    public void setUp() {
        var context = new PipelineContext();
        context.registerComputation(Computation.builder()
                .setComputationId("writer")
                .setProcessFunction(new MessageFilter())
                .build());
        harness = TestComputationHarness.builder()
                .setPipelineContext(context)
                .setPipelineSpec(getClass().getClassLoader().getResourceAsStream("pipeline.yson"))
                .build();
    }

    private static ExtendedMessage eventMessage(String key, String data) {
        return ExtendedMessage.builder()
                .setMessageId(GUID.create().toString())
                .setStreamId("event_in")
                .setKey(new PayloadBuilder(KEY_SCHEMA)
                        // Any deterministic per-key hash works offline; live, the engine computes farm_hash.
                        .set("hash", Integer.toUnsignedLong(key.hashCode()))
                        .set("key", key)
                        .finish())
                .setPayload(new PayloadBuilder(EVENT_SCHEMA)
                        .set("key", key)
                        .set("data", data)
                        .finish())
                .build();
    }

    @Test
    public void testBlacklistedKeyIsDropped() {
        var response = harness.doProcess(TestDoProcessRequest.builder("writer")
                .setMessages(List.of(eventMessage("bad", "1")))
                .build());

        assertTrue(response.getOutputMessagesFlatten().isEmpty());
    }

    @Test
    public void testGoodKeyGetsUppercasedData() {
        var response = harness.doProcess(TestDoProcessRequest.builder("writer")
                .setMessages(List.of(eventMessage("good_0", "hello")))
                .build());

        var messages = response.getOutputMessagesFlatten();
        assertEquals(1, messages.size());
        assertEquals("event_out", messages.get(0).getStreamId());
        assertEquals("good_0", messages.get(0).get("key", String.class));
        assertEquals("hello", messages.get(0).get("data", String.class));
        assertEquals("HELLO", messages.get(0).get("data_upper", String.class));
    }

    @Test
    public void testOneBadAndTwoGoodRowsInOneBatch() {
        // The scenario's own feed data (see the README): "bad" is dropped, both good_* rows pass
        // through with their derived column.
        var response = harness.doProcess(TestDoProcessRequest.builder("writer")
                .setMessages(List.of(
                        eventMessage("good_0", "0"),
                        eventMessage("bad", "1"),
                        eventMessage("good_1", "2")))
                .build());

        var keys = response.getOutputMessagesFlatten().stream()
                .map(message -> message.get("key", String.class))
                .toList();
        assertEquals(List.of("good_0", "good_1"), keys);
    }
}
