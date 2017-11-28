package eu.fbk.fm.tweetframe.pipeline.text.filtered;

import com.google.common.collect.ImmutableMap;
import eu.fbk.fm.tweetframe.pipeline.text.CleanUpText;
import ixa.kaflib.KAFDocument;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;
import java.util.stream.Stream;

public abstract class AbstractFilterTextStream<T, R> extends RichMapFunction<T, R> {

    public final static int UNKNOWN_TOKEN = 0;
    public final static int UNKNOWN_POS = 0;
    public final static String UNKNOWN_STR = "<unk>";

    private String[] posTags;
    private String[] words;

    protected transient ImmutableMap<String, Integer> posTagDict;
    protected transient ImmutableMap<String, Integer> wordsDict;

    public AbstractFilterTextStream(String[] posTags, String[] words) {
        this.posTags = posTags;
        this.words = words;
    }

    @Override
    public void open(Configuration configuration) {
        this.posTagDict = arrayToDict(posTags);
        this.wordsDict = arrayToDict(words);
        this.posTags = null;
        this.words = null;
    }

    private ImmutableMap<String, Integer> arrayToDict(String[] arr) {
        ImmutableMap.Builder<String, Integer> dictBuilder = new ImmutableMap.Builder<>();
        for (int i = 0; i < arr.length; i++) {
            dictBuilder.put(arr[i], i);
        }
        return dictBuilder.build();
    }

    protected Stream<CleanUpText.SimpleTerm> getTokenStream(T document) {
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
