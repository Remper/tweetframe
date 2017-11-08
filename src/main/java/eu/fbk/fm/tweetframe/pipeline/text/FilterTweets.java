package eu.fbk.fm.tweetframe.pipeline.text;

import com.google.gson.JsonObject;
import eu.fbk.fm.tweetframe.utils.flink.JsonObjectProcessor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Filters out tweets that we are not interested in and add retweets
 */
public class FilterTweets implements FlatMapFunction<JsonObject, JsonObject>, JsonObjectProcessor {

    private static final long serialVersionUID = 1L;

    private final String[] allowedLanguages;

    public FilterTweets() {
        this(new String[0]);
    }

    public FilterTweets(String[] allowedLanguages) {
        this.allowedLanguages = allowedLanguages;
    }

    @Override
    public void flatMap(JsonObject status, Collector<JsonObject> out) {
        //Recursively process retweeted object
        JsonObject retweetedObject = status.getAsJsonObject("retweeted_status");
        if (retweetedObject != null) {
            this.flatMap(retweetedObject, out);
        }

        String text = get(status, String.class, "text");
        if (text == null) {
            return;
        }

        if (!isLangAllowed(get(status, String.class, "lang"))) {
            return;
        }

        out.collect(status);
    }

    private boolean isLangAllowed(String lang) {
        if (allowedLanguages.length == 0) {
            return true;
        }

        for (String allowedLang : allowedLanguages) {
            if (allowedLang.equalsIgnoreCase(lang)) {
                return true;
            }
        }

        return false;
    }
}
