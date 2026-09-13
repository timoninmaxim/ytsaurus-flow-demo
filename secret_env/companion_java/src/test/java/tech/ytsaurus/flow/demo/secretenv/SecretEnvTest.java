package tech.ytsaurus.flow.demo.secretenv;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Message;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.flow.testutils.TestComputationHarness;
import tech.ytsaurus.flow.testutils.TestDoProcessRequest;
import tech.ytsaurus.typeinfo.TiType;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Offline tests of the checker, mirroring the Go variant's {@code main_test.go}: the reported
 * columns are pinned for the correct, wrong and absent environment shapes. The JVM has no
 * {@code setenv}, so the environment is injected into {@link SecretCheckerFunction} as a lookup
 * over a plain map — the live chain (vault → job env → JVM inheritance) is proven by the live run.
 */
public class SecretEnvTest {

    private static final TableSchema KEY_SCHEMA = TableSchema.builder()
            .addValue("hash", TiType.uint64())
            .addValue("key", TiType.string())
            .build();

    private static final TableSchema EVENTS_SCHEMA = TableSchema.builder()
            .addValue("key", TiType.string())
            .build();

    private static Payload keyOf(String key) {
        // Any deterministic per-key hash works offline; live, the engine computes farm_hash.
        return new PayloadBuilder(KEY_SCHEMA)
                .set("hash", Integer.toUnsignedLong(key.hashCode()))
                .set("key", key)
                .finish();
    }

    private static ExtendedMessage eventMessage(String key) {
        return ExtendedMessage.builder()
                .setMessageId(GUID.create().toString())
                .setStreamId("events")
                .setKey(keyOf(key))
                .setPayload(new PayloadBuilder(EVENTS_SCHEMA)
                        .set("key", key)
                        .finish())
                .build();
    }

    private static TestComputationHarness harnessWithEnv(Map<String, String> env) {
        var context = new PipelineContext();
        context.registerComputation(Computation.builder()
                .setComputationId("checker")
                .setProcessFunction(new SecretCheckerFunction(env::get))
                .build());
        return TestComputationHarness.builder()
                .setPipelineContext(context)
                .setPipelineSpec(SecretEnvTest.class.getClassLoader().getResourceAsStream("pipeline.yson"))
                .build();
    }

    private static Message observe(Map<String, String> env, String key) {
        var response = harnessWithEnv(env).doProcess(TestDoProcessRequest.builder("checker")
                .setMessages(List.of(eventMessage(key)))
                .build());
        var observations = response.getOutputMessagesFlatten();
        assertEquals(1, observations.size());
        assertEquals("observations", observations.get(0).getStreamId());
        return observations.get(0);
    }

    @Test
    public void reportsTheSecretAndTheVaultName() {
        var observation = observe(Map.of(
                "YT_MY_SECRET", "5",
                "YT_SECURE_VAULT", "{\"YT_MY_SECRET\"=\"5\";}"), "pos-1");

        assertEquals("pos-1", observation.get("key", String.class));
        assertEquals("5", observation.get("secret", String.class));
        assertEquals("true", observation.get("vault_carries_name", String.class));
    }

    @Test
    public void reportsAWrongValueVerbatim() {
        var observation = observe(Map.of(
                "YT_MY_SECRET", "wrong",
                "YT_SECURE_VAULT", "{\"YT_MY_SECRET\"=\"wrong\";}"), "neg-1");

        assertEquals("wrong", observation.get("secret", String.class));
        assertEquals("true", observation.get("vault_carries_name", String.class));
    }

    @Test
    public void reportsAnAbsentEnvironment() {
        var observation = observe(Map.of(), "neg-2");

        assertEquals("<unset>", observation.get("secret", String.class));
        assertEquals("false", observation.get("vault_carries_name", String.class));
    }
}
