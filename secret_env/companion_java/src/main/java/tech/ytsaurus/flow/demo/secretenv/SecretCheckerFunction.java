package tech.ytsaurus.flow.demo.secretenv;

import java.util.function.UnaryOperator;

import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;

/**
 * Reports the secret as observed in the companion JVM's own environment: for every input message
 * it writes the value of {@code YT_MY_SECRET} and whether the inherited raw {@code YT_SECURE_VAULT}
 * text mentions the name (a substring probe, diagnostic only) into the "observations" stream.
 *
 * <p>Like the Python and Go checkers it <em>reports</em> instead of crashing — an exception in a
 * companion is retried forever. The verification compares the reported value from outside: the
 * value in the output queue can only have come from this process's environment. The two columns
 * separate the links of the chain: {@code vault_carries_name = "true"} with an empty secret would
 * mean the vault reached the job but the re-export or the JVM inheritance broke; both empty would
 * mean the vault never reached the job at all.
 *
 * <p>The environment is injectable because the JVM has no {@code setenv}: the offline test passes
 * a fake lookup, production uses {@link System#getenv(String)}.
 */
public class SecretCheckerFunction implements RowFunction {

    static final String SECRET_ENV_NAME = "YT_MY_SECRET";
    static final String VAULT_ENV_NAME = "YT_SECURE_VAULT";

    private final UnaryOperator<String> env;

    public SecretCheckerFunction() {
        this(System::getenv);
    }

    SecretCheckerFunction(UnaryOperator<String> env) {
        this.env = env;
    }

    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        String secret = env.apply(SECRET_ENV_NAME);
        String vault = env.apply(VAULT_ENV_NAME);

        output.addMessage(ctx.createMessageBuilder("observations")
                .set("key", message.get("key", String.class))
                .set("secret", secret == null ? "<unset>" : secret)
                .set("vault_carries_name", String.valueOf(vault != null && vault.contains(SECRET_ENV_NAME)))
                .finish());
    }
}
