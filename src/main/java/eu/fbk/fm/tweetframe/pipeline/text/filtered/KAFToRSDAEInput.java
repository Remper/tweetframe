package eu.fbk.fm.tweetframe.pipeline.text.filtered;

import org.tensorflow.example.*;

/**
 * Puts terms from a KAFDocument into a Tensorflow Example
 */
public class KAFToRSDAEInput<T> extends AbstractFilterTextStream<T, byte[]> {

    public KAFToRSDAEInput(String[] posTags, String[] words) {
        super(posTags, words);
    }

    @Override
    public byte[] map(T document) throws Exception {
        Int64List.Builder posTags = Int64List.newBuilder();
        Int64List.Builder text = Int64List.newBuilder();
        getTokenStream(document).forEach(term -> {
            posTags.addValue(posTagDict.getOrDefault(term.pos, UNKNOWN_POS));
            text.addValue(wordsDict.getOrDefault(term.lemma, UNKNOWN_TOKEN));
        });

        Features features = Features.newBuilder()
                .putFeature("pos", Feature.newBuilder().setInt64List(posTags.build()).build())
                .putFeature("text", Feature.newBuilder().setInt64List(text.build()).build())
                .build();
        return Example.newBuilder().setFeatures(features).build().toByteArray();
    }
}
