package eu.fbk.fm.tweetframe.pipeline.tweets;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import eu.fbk.fm.tweetframe.utils.flink.JsonObjectProcessor;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * Filters out tweets that do not contain any images
 */
public class WithImagesFilter implements FilterFunction<JsonObject>, JsonObjectProcessor {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean filter(JsonObject object) {
        try {
            JsonArray mediaEntities = object.getAsJsonObject("entities").getAsJsonArray("media");
            return mediaEntities.size() > 0;
        } catch (Exception e) {
            return false;
        }
    }
}
