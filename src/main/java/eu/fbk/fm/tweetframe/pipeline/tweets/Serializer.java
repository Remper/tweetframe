package eu.fbk.fm.tweetframe.pipeline.tweets;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by remper on 03/11/2017.
 */
public final class Serializer implements MapFunction<Tuple2<Long, JsonObject>, Tuple2<Long, String>> {

    private static final long serialVersionUID = 1L;

    private static final Gson GSON = new Gson();

    @Override
    public Tuple2<Long, String> map(Tuple2<Long, JsonObject> value) throws Exception {
        return new Tuple2<>(value.f0, GSON.toJson(value.f1));
    }
}
