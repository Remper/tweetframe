package eu.fbk.fm.tweetframe.pipeline;

import eu.fbk.fm.tweetframe.pipeline.text.AnnotateLocal;
import eu.fbk.fm.tweetframe.pipeline.tweets.*;
import eu.fbk.fm.tweetframe.utils.flink.JsonObjectProcessor;
import eu.fbk.fm.tweetframe.utils.flink.RobustTsvOutputFormat;
import eu.fbk.fm.tweetframe.utils.flink.TextInputFormat;
import eu.fbk.utils.core.CommandLine;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Extracts frames from tweets locally
 */
public class ExtractFramesLocal implements JsonObjectProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractFramesLocal.class);
    private static final String TWEETS_PATH = "tweets-path";
    private static final String RESULTS_PATH = "results-path";
    private static final String CONFIG_PATH = "config-path";

    private void start(Path input, Path output, File configFile) throws Exception {
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
        final DataSet<Tuple3<String, Integer, String>> results = text
                .flatMap(new Deserializer())
                .flatMap(new FilterTweets(new String[]{"en"}))
                .filter(new WithImagesFilter())
                .flatMap(new TextExtractorV2())
                .flatMap(new AnnotateLocal(configFile))
                .flatMap(new FilterAnnotatedSentences(new File(output.getPath())));

        results
                .output(new RobustTsvOutputFormat<>(new Path(output, "frames"))).setParallelism(1);

        results
                .filter((FilterFunction<Tuple3<String, Integer, String>>) value -> value.f1 >= FilterAnnotatedSentences.HIGH_PRIORITY)
                .output(new RobustTsvOutputFormat<>(new Path(output, "top_frames"))).setParallelism(1);

        env.execute();
    }

    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption("t", TWEETS_PATH,
                        "specifies the directory from which to get a stream of tweets", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true)
                .withOption("r", RESULTS_PATH,
                    "specifies the directory for the results and intermediate datasets", "DIRECTORY",
                    CommandLine.Type.STRING, true, false, true)
                .withOption("c", CONFIG_PATH,
                        "file with Pikes configuration", "CONFIG",
                        CommandLine.Type.STRING, true, false, true);
    }

    public static void main(String[] args) throws Exception {
        ExtractFramesLocal extractor = new ExtractFramesLocal();

        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            //noinspection ConstantConditions
            final Path tweetsPath = new Path(cmd.getOptionValue(TWEETS_PATH, String.class));

            //noinspection ConstantConditions
            final Path resultsPath = new Path(cmd.getOptionValue(RESULTS_PATH, String.class));

            //noinspection ConstantConditions
            final File configFile = new File(cmd.getOptionValue(CONFIG_PATH, String.class));

            extractor.start(tweetsPath, resultsPath, configFile);
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }
}
