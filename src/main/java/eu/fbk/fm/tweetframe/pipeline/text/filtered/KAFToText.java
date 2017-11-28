package eu.fbk.fm.tweetframe.pipeline.text.filtered;

import java.util.List;
import java.util.stream.Collectors;

public class KAFToText<T> extends AbstractFilterTextStream<T, String> {

    public KAFToText(String[] posTags, String[] words) {
        super(posTags, words);
    }

    @Override
    public String map(T document) throws Exception {
        List<String> tokens = getTokenStream(document)
                .map(term -> {
                    String value = wordsDict.containsKey(term.lemma) ? term.lemma : UNKNOWN_STR;
                    if (!posTagDict.isEmpty()) {
                        value += "@" + (posTagDict.containsKey(term.pos) ? term.pos : UNKNOWN_STR);
                    }
                    return value;
                })
                .collect(Collectors.toList());
        return String.join(" ", tokens);
    }
}
