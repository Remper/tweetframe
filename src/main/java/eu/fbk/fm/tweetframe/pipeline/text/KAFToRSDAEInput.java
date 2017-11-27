package eu.fbk.fm.tweetframe.pipeline.text;

import com.google.common.collect.ImmutableMap;
import ixa.kaflib.KAFDocument;
import org.apache.flink.api.common.functions.MapFunction;
import org.tensorflow.example.*;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * Puts terms from a KAFDocument into a Tensorflow Example
 */
public class KAFToRSDAEInput<T> implements MapFunction<T, Example> {

    public final static long UNKNOWN_TOKEN = 0;
    public final static long UNKNOWN_POS = 0;

    private ImmutableMap<String, Long> posTagDict;
    private ImmutableMap<String, Long> wordDict;

    public KAFToRSDAEInput(ImmutableMap<String, Long> posTagDict, ImmutableMap<String, Long> wordDict) {
        this.posTagDict = posTagDict;
        this.wordDict = wordDict;
    }

    @Override
    public Example map(T document) throws Exception {
        Int64List.Builder posTags = Int64List.newBuilder();
        Int64List.Builder text = Int64List.newBuilder();
        getTokenStream(document).forEach(term -> {
            posTags.addValue(posTagDict.getOrDefault(term.pos, UNKNOWN_POS));
            text.addValue(wordDict.getOrDefault(term.lemma, UNKNOWN_TOKEN));
        });

        Features features = Features.newBuilder()
                .putFeature("pos", Feature.newBuilder().setInt64List(posTags.build()).build())
                .putFeature("text", Feature.newBuilder().setInt64List(text.build()).build())
                .build();
        return Example.newBuilder().setFeatures(features).build();
    }

    private Stream<CleanUpText.SimpleTerm> getTokenStream(T document) {
        if (document instanceof KAFDocument) {
            return ((KAFDocument) document).getTerms().stream().map(CleanUpText.SimpleTerm::fromTerm);
        }
        if (document instanceof Collection<?>) {
            return ((Collection<?>) document).stream()
                    .filter(term -> term instanceof CleanUpText.SimpleTerm)
                    .map(term -> (CleanUpText.SimpleTerm) term);
        }
        throw new IllegalArgumentException("An argument is not a convertable into the SimpleTerm stream");
    }
}
