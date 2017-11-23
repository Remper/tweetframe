package eu.fbk.fm.tweetframe.pipeline;

import eu.fbk.fm.tweetframe.pipeline.tweets.Deserializer;
import eu.fbk.fm.tweetframe.pipeline.tweets.FilterTweets;
import eu.fbk.fm.tweetframe.pipeline.tweets.TextExtractorV2;
import eu.fbk.fm.tweetframe.utils.flink.JsonObjectProcessor;
import eu.fbk.fm.tweetframe.utils.flink.TextInputFormat;
import eu.fbk.fm.tweetframe.utils.flink.azure.AzureStorageIOConfig;
import eu.fbk.fm.tweetframe.utils.flink.azure.TextOutputFormat;
import eu.fbk.utils.core.CommandLine;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts text from tweets
 */
public class ExtractTextToAzure implements JsonObjectProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractTextToAzure.class);

    private static final String OUTPUT_CONFIG = "output-config";
    private static final String TWEETS_PATH = "tweets-path";
    private static final String TEXT_PATH = "text-path";

    private DataSet<String> getInput(ExecutionEnvironment env, Path input, Configuration parameters) {
        parameters.setBoolean("recursive.file.enumeration", true);

        return new DataSource<>(
                env,
                new TextInputFormat(input),
                BasicTypeInfo.STRING_TYPE_INFO,
                Utils.getCallLocationName()
        ).withParameters(parameters);
    }

    private void tweets(Path input, Configuration parameters) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //Deserialize and convert
        getInput(env, input, parameters)
                .flatMap(new Deserializer())
                .flatMap(new FilterTweets(new String[]{"en"}))
                .flatMap(new TextExtractorV2())
                .output(new TextOutputFormat<>()).withParameters(parameters);

        env.execute();
    }

    private void text(Path input, Configuration parameters) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        getInput(env, input, parameters)
                .output(new TextOutputFormat<>()).withParameters(parameters);

        env.execute();
    }

    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption("t", TWEETS_PATH,
                        "specifies the directory from which to get a stream of tweets", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, false)
                .withOption(null, TEXT_PATH,
                        "specifies the directory from which to get a stream of tweets", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, false)
                .withOption("o", OUTPUT_CONFIG,
                        "Output config", "STRING",
                        CommandLine.Type.STRING, true, false, true);
    }

    public static void main(String[] args) throws Exception {
        ExtractTextToAzure extractor = new ExtractTextToAzure();

        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            final String tweetsPath = cmd.getOptionValue(TWEETS_PATH, String.class);
            final String textPath = cmd.getOptionValue(TEXT_PATH, String.class);

            Configuration parameters = AzureStorageIOConfig.confFromJson(cmd.getOptionValue(OUTPUT_CONFIG, String.class));

            if (tweetsPath != null) {
                extractor.tweets(new Path(tweetsPath), parameters);
            } else if (textPath != null) {
                extractor.text(new Path(textPath), parameters);
            } else {
                throw new Exception("One of input options should be set");
            }
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }
}
