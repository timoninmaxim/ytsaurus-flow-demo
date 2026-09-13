package tech.ytsaurus.flow.demo.wordcountsync;

import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.flow.state.ExternalStateDescriptor;
import tech.ytsaurus.flow.state.StateDescriptors;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Counts word occurrences in the external state {@code /state}, backed by the word_counts table.
 *
 * <p>Words from the {@code stop_words} parameter are dropped entirely; of the rest, words shorter
 * than {@code min_word_length} are skipped and emitted into the "skipped" stream, whose sink
 * writes them into the skipped-words table inside the same epoch transaction that commits the
 * counts. As in the Python and Go variants, the stop words travel in the computation's spec
 * parameters — the Java SDK has no counterpart of the C++ companion-hosted
 * {@code TStopWordsResource}.
 */
public class WordCount implements RowFunction {

    static final ExternalStateDescriptor COUNT_STATE = StateDescriptors.external("/state");

    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        String word = message.get("word", String.class);
        if (word == null) {
            return;
        }

        var parameters = ctx.getComputationParameters();
        YTreeNode stopWords = parameters.get("stop_words");
        if (stopWords != null) {
            for (YTreeNode stopWord : stopWords.asList()) {
                if (stopWord.stringValue().equals(word)) {
                    return;
                }
            }
        }

        long minWordLength = 0;
        YTreeNode minWordLengthNode = parameters.get("min_word_length");
        if (minWordLengthNode != null) {
            minWordLength = minWordLengthNode.longValue();
        }
        if (word.length() < minWordLength) {
            output.addMessage(ctx.createMessageBuilder("skipped")
                    .set("word", word)
                    .set("length", (long) word.length())
                    .finish());
            return;
        }

        var state = ctx.getState(COUNT_STATE, message);
        // A key with no count yet is not always an absent row: live, the state manager hands
        // the companion a present row with the key columns set and "count" null, so the null
        // check must be per column, as in the C++ variant's optional<i64>.value_or(0).
        // getOrDefault() covers the truly-absent case with an all-null row of the state schema.
        Payload row = state.getOrDefault();
        Long count = row.get("count", Long.class);
        // Only "count" is set in the written-back row; the state manager fills the key columns
        // from the grouping key, as in the C++ variant.
        state.set(new PayloadBuilder(row.getSchema())
                .set("count", (count == null ? 0 : count) + 1)
                .finish());
    }
}
