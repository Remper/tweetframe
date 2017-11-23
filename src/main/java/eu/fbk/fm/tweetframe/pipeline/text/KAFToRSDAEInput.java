package eu.fbk.fm.tweetframe.pipeline.text;

import com.google.common.collect.ImmutableMap;
import ixa.kaflib.KAFDocument;
import org.apache.flink.api.common.functions.MapFunction;
import org.tensorflow.example.*;

/**
 * Puts terms from a KAFDocument into a Tensorflow Example
 */
public class KAFToRSDAEInput implements MapFunction<KAFDocument, Example> {

    public final static long UNKNOWN_TOKEN = 0;
    public final static long UNKNOWN_POS = 0;

    ImmutableMap<String, Long> posTagDict;
    ImmutableMap<String, Long> wordDict;

    public KAFToRSDAEInput(ImmutableMap<String, Long> posTagDict, ImmutableMap<String, Long> wordDict) {
        this.posTagDict = posTagDict;
        this.wordDict = wordDict;
    }

    @Override
    public Example map(KAFDocument kafDocument) throws Exception {
        Int64List.Builder posTags = Int64List.newBuilder();
        Int64List.Builder text = Int64List.newBuilder();
        kafDocument.getTerms().forEach(term -> {
            posTags.addValue(posTagDict.getOrDefault(term.getPos(), UNKNOWN_POS));
            text.addValue(wordDict.getOrDefault(term.getLemma(), UNKNOWN_TOKEN));
        });

        Features features = Features.newBuilder()
                .putFeature("pos", Feature.newBuilder().setInt64List(posTags.build()).build())
                .putFeature("text", Feature.newBuilder().setInt64List(text.build()).build())
                .build();
        return Example.newBuilder().setFeatures(features).build();
    }
}
