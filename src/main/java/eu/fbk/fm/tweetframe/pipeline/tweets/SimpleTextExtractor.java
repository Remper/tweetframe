package eu.fbk.fm.tweetframe.pipeline.tweets;

import com.google.gson.JsonObject;
import eu.fbk.fm.tweetframe.utils.flink.JsonObjectProcessor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;

/**
 * Extracts text from tweets by replacing all tweet entities with a single word
 */
public class SimpleTextExtractor implements FlatMapFunction<JsonObject, Tuple1<String>>, JsonObjectProcessor {

    private static final long serialVersionUID = 1L;

    @Override
    public void flatMap(JsonObject status, Collector<Tuple1<String>> out) {
        String text = get(status, String.class, "text");

        out.collect(new Tuple1<>(process(text)));
    }

    private String process(String originalText) {
        //Replace URLs, Mentions and Hashtags
        return originalText
                .replaceAll("^RT ", "")
                .replaceAll("https?://[^\\s]+", "url")
                .replaceAll("@[^\\s]+", "mention")
                .replaceAll("#[^\\s]+", "hashtag");
    }
}
