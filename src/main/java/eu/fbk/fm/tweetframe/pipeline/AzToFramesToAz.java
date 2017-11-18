package eu.fbk.fm.tweetframe.pipeline;

import eu.fbk.fm.tweetframe.pipeline.text.AnnotateLocal;
import eu.fbk.fm.tweetframe.pipeline.text.FrameDataFromKAF;
import eu.fbk.fm.tweetframe.pipeline.tweets.FilterAnnotatedSentencesV2;
import eu.fbk.fm.tweetframe.utils.flink.JsonObjectProcessor;
import eu.fbk.fm.tweetframe.utils.flink.azure.AzureStorageIOConfig;
import eu.fbk.fm.tweetframe.utils.flink.azure.BlobInputFormat;
import eu.fbk.fm.tweetframe.utils.flink.azure.BlobOutputFormat;
import eu.fbk.utils.core.CommandLine;
import ixa.kaflib.KAFDocument;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Gets text from Azure, extracts frames and stores results back to Azure
 */
public class AzToFramesToAz implements JsonObjectProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(AzToFramesToAz.class);

    private static final String INPUT_CFG = "input-config";
    private static final String OUTPUT_CFG = "output-config";
    private static final String PIPELINE_PATH = "pipeline-path";

    private void start(Configuration input, Configuration output, File pipelinePath) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<String> text = new DataSource<>(
                env,
                new BlobInputFormat(),
                BasicTypeInfo.STRING_TYPE_INFO,
                Utils.getCallLocationName()
        ).withParameters(input);

        //Deserialize and convert
        final DataSet<Tuple2<KAFDocument, Integer>> results = text
                .flatMap(new AnnotateLocal(pipelinePath))
                .flatMap(new FilterAnnotatedSentencesV2(pipelinePath));

        //results
        //        .output(new BlobOutputFormat<>()).withParameters(parameters).setParallelism(1);

        Configuration verbalizedOutput = output.clone();
        verbalizedOutput.setString(AzureStorageIOConfig.AZURE_BLOB_PREFIX, "verbalized");
        results
                .flatMap(new FrameDataFromKAF())
                .output(new BlobOutputFormat<>()).withParameters(verbalizedOutput);

        env.execute();
    }

    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption("i", INPUT_CFG,
                        "specifies the directory from which to get a stream of tweets", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true)
                .withOption("o", OUTPUT_CFG,
                    "specifies the directory for the results and intermediate datasets", "DIRECTORY",
                    CommandLine.Type.STRING, true, false, true)
                .withOption("p", PIPELINE_PATH,
                        "file with Pikes configuration", "CONFIG",
                        CommandLine.Type.STRING, true, false, true);
    }

    public static void main(String[] args) throws Exception {
        AzToFramesToAz extractor = new AzToFramesToAz();

        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            Configuration inputCfg = AzureStorageIOConfig.confFromJson(cmd.getOptionValue(INPUT_CFG, String.class));
            Configuration outputCfg = AzureStorageIOConfig.confFromJson(cmd.getOptionValue(OUTPUT_CFG, String.class));

            //noinspection ConstantConditions
            final File configFile = new File(cmd.getOptionValue(PIPELINE_PATH, String.class));

            extractor.start(inputCfg, outputCfg, configFile);
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }
}
