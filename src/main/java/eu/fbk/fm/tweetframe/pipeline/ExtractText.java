package eu.fbk.fm.tweetframe.pipeline;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import eu.fbk.fm.tweetframe.pipeline.text.FilterTweets;
import eu.fbk.fm.tweetframe.pipeline.text.V2TextExtractor;
import eu.fbk.fm.tweetframe.pipeline.tweets.Deserializer;
import eu.fbk.fm.tweetframe.utils.flink.JsonObjectProcessor;
import eu.fbk.fm.tweetframe.utils.flink.LimitedSizeTsvOutputFormat;
import eu.fbk.fm.tweetframe.utils.flink.TextInputFormat;
import eu.fbk.utils.core.CommandLine;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

/**
 * Extracts text from tweets
 */
public class ExtractText implements JsonObjectProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractText.class);

    private static final String RESULTS_PATH = "results-path";
    private static final String TWEETS_PATH = "tweets-path";

    private void start(Path input, Path output) throws Exception {
        final LimitedSizeTsvOutputFormat<Tuple1<String>> outputFormat = new LimitedSizeTsvOutputFormat<>(output);
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final Configuration parameters = new Configuration();
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
            .flatMap(new V2TextExtractor())
            .output(outputFormat.limit(20000));

        env.execute();
    }

    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption("t", TWEETS_PATH,
                        "specifies the directory from which to get a stream of tweets", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true)
                .withOption("r", RESULTS_PATH,
                        "specifies the directory to which the results will be saved (in this case the db params are not required)", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true);
    }

    public static void main(String[] args) throws Exception {
        ExtractText extractor = new ExtractText();

        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            //noinspection ConstantConditions
            final Path tweetsPath = new Path(cmd.getOptionValue(TWEETS_PATH, String.class));

            //noinspection ConstantConditions
            final Path results = new Path(cmd.getOptionValue(RESULTS_PATH, String.class));

            extractor.start(tweetsPath, results);
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }
}
