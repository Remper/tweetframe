package eu.fbk.fm.tweetframe.pipeline;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import eu.fbk.fm.tweetframe.pipeline.text.FilterTweets;
import eu.fbk.fm.tweetframe.pipeline.text.TextExtractorV2;
import eu.fbk.fm.tweetframe.pipeline.tweets.Deserializer;
import eu.fbk.fm.tweetframe.utils.flink.JsonObjectProcessor;
import eu.fbk.fm.tweetframe.utils.flink.TextInputFormat;
import eu.fbk.fm.tweetframe.utils.flink.azure.BlobOutputFormat;
import eu.fbk.utils.core.CommandLine;
import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;

/**
 * Extracts text from tweets
 */
public class ExtractTextToAzure implements JsonObjectProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractTextToAzure.class);

    private static final String OUTPUT_CONFIG = "output-config";
    private static final String TWEETS_PATH = "tweets-path";

    private void start(Path input, Configuration parameters) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        parameters.setBoolean("recursive.file.enumeration", true);

        final DataSet<String> text = new DataSource<>(
                env,
                new TextInputFormat(input),
                BasicTypeInfo.STRING_TYPE_INFO,
                Utils.getCallLocationName()
        ).withParameters(parameters);

        //Deserialize and convert
        text
            .flatMap(new Deserializer())
            .flatMap(new FilterTweets(new String[]{"en"}))
            .flatMap(new TextExtractorV2())
            .output(new BlobOutputFormat<>()).withParameters(parameters);

        env.execute();
    }

    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption("t", TWEETS_PATH,
                        "specifies the directory from which to get a stream of tweets", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true)
                .withOption("o", OUTPUT_CONFIG,
                        "Ouput config", "STRING",
                        CommandLine.Type.STRING, true, false, true);
    }

    public static void main(String[] args) throws Exception {
        ExtractTextToAzure extractor = new ExtractTextToAzure();

        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            //noinspection ConstantConditions
            final Path tweetsPath = new Path(cmd.getOptionValue(TWEETS_PATH, String.class));

            Configuration parameters = new Configuration();

            //noinspection ConstantConditions
            new Gson()
                    .fromJson(new FileReader(cmd.getOptionValue(OUTPUT_CONFIG, String.class)), JsonObject.class)
                    .entrySet()
                    .forEach(entry -> {
                        JsonPrimitive primitive = entry.getValue().getAsJsonPrimitive();
                        if (primitive.isBoolean()) {
                            parameters.setBoolean(entry.getKey(), primitive.getAsBoolean());
                            return;
                        }

                        parameters.setString(entry.getKey(), primitive.getAsString());
                    });

            extractor.start(tweetsPath, parameters);
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }
}
