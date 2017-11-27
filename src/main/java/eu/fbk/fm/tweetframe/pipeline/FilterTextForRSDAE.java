package eu.fbk.fm.tweetframe.pipeline;

import com.google.common.collect.ImmutableMap;
import eu.fbk.fm.tweetframe.pipeline.text.AnnotateLocal;
import eu.fbk.fm.tweetframe.pipeline.text.CleanUpText;
import eu.fbk.fm.tweetframe.pipeline.text.KAFToRSDAEInput;
import eu.fbk.fm.tweetframe.utils.flink.TextInputFormat;
import eu.fbk.fm.tweetframe.utils.flink.azure.AzureStorageIOConfig;
import eu.fbk.fm.tweetframe.utils.flink.azure.BlobInputFormat;
import eu.fbk.fm.tweetframe.utils.flink.azure.TFRecordOutputFormat;
import eu.fbk.fm.tweetframe.utils.flink.azure.TextOutputFormat;
import eu.fbk.utils.core.CommandLine;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.example.Example;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Filters text against Visual Genome dataset and outputs it back in TFRecord format including the POS tag for each word
 */
public class FilterTextForRSDAE {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterTextForRSDAE.class);

    private static final String INPUT_CFG = "input-config";
    private static final String OUTPUT_CFG = "output-config";
    private static final String PIPELINE_PATH = "pipeline-path";
    private static final String TENSORFLOW = "tensorflow";

    private static final String POS_TAG_DICT = "postag.dict";
    private static final String WORD_DICT = "word.dict";
    private static final String ANNOTATED_INPUT = "annotated-input";
    private static final String TENSORFLOW_INPUT = "tensorflow-input";
    private static final int ANNOTATED_INPUT_BREAK = 10000000;

    private void start(String input, Configuration output, File pipelinePath) throws Exception {
        final ExecutionEnvironment env = getExecutionEnvironment();

        //Annotate and cleanup
        final DataSet<Example> results = getInput(input, env)
                .map((MapFunction<String, List<CleanUpText.SimpleTerm>>) value -> {
                    String[] tokens = value.split(" ");
                    List<CleanUpText.SimpleTerm> result = new LinkedList<>();
                    for (String token : tokens) {
                        result.add(CleanUpText.SimpleTerm.fromString(token));
                    }
                    return result;
                })
                .map(new KAFToRSDAEInput<>(instantiatePosTagDictionary(pipelinePath), instantiateWordDictionary(pipelinePath)));

        //Output configuration for TFRecords
        Configuration annotatedInputConf = output.clone();
        annotatedInputConf.setString(AzureStorageIOConfig.AZURE_BLOB_PREFIX, TENSORFLOW_INPUT);
        annotatedInputConf.setInteger(AzureStorageIOConfig.AZURE_BLOB_BREAK, ANNOTATED_INPUT_BREAK);
        results.output(new TFRecordOutputFormat()).withParameters(annotatedInputConf).setParallelism(1);

        env.execute();
    }

    private void calculateStats(String input, Configuration output, File pipelinePath) throws Exception {
        final ExecutionEnvironment env = getExecutionEnvironment();

        //Annotate and cleanup
        final DataSet<List<CleanUpText.SimpleTerm>> results = getInput(input, env)
                .flatMap(new AnnotateLocal(pipelinePath))
                .map(new CleanUpText());

        //Output configuration for annotated text
        Configuration annotatedInputConf = output.clone();
        annotatedInputConf.setString(AzureStorageIOConfig.AZURE_BLOB_PREFIX, ANNOTATED_INPUT);
        annotatedInputConf.setInteger(AzureStorageIOConfig.AZURE_BLOB_BREAK, ANNOTATED_INPUT_BREAK);
        if (!TextOutputFormat.exists(annotatedInputConf)) {
            //Pipeline for annotated text
            results.map(value -> {
                StringBuilder sb = new StringBuilder();
                value.forEach(simpleTerm -> {
                    if (sb.length() > 0) {
                        sb.append(" ");
                    }
                    sb.append(simpleTerm.toString());
                });
                return sb.toString();
            }).returns(String.class).output(new TextOutputFormat<>()).withParameters(annotatedInputConf).setParallelism(20);
        }

        //Output configuration for dictionary
        Configuration dictionaryConf = output.clone();
        dictionaryConf.setString(AzureStorageIOConfig.AZURE_BLOB_PREFIX, WORD_DICT);
        dictionaryConf.setInteger(AzureStorageIOConfig.AZURE_BLOB_BREAK, 0);
        if (!TextOutputFormat.exists(dictionaryConf)) {
            //Pipeline for dictionary
            results
                    //Flatten terms in each sentence, extract lemma
                    .flatMap((TermMapper) (value, out) -> value.forEach(simpleTerm -> out.collect(new Tuple2<>(simpleTerm.lemma, 1))))
                    .returns(new TypeHint<Tuple2<String, Integer>>() {})
                    //Group by lemma, sum
                    .groupBy(0).sum(1)
                    //Filter lemmas that have more than a thousand entries
                    .filter((FilterFunction<Tuple2<String, Integer>>) value -> value.f1 > 100)
                    //Output
                    .output(new TextOutputFormat<>()).withParameters(dictionaryConf).setParallelism(1);
        }

        //Output configuration for pos tag dictionary
        Configuration posTagsConf = output.clone();
        posTagsConf.setString(AzureStorageIOConfig.AZURE_BLOB_PREFIX, POS_TAG_DICT);
        posTagsConf.setInteger(AzureStorageIOConfig.AZURE_BLOB_BREAK, 0);
        if (!TextOutputFormat.exists(posTagsConf)) {
            //Pipeline for pos tag dictionary
            results
                    //Flatten terms in each sentence, extract pos tag
                    .flatMap((TermMapper) (value, out) -> value.forEach(simpleTerm -> out.collect(new Tuple2<>(simpleTerm.pos, 1))))
                    .returns(new TypeHint<Tuple2<String, Integer>>() {})
                    //Group by pos tag, sum
                    .groupBy(0).sum(1)
                    //Output
                    .output(new TextOutputFormat<>()).withParameters(posTagsConf).setParallelism(1);
        }

        env.execute();
    }

    private DataSet<String> getInput(String input, ExecutionEnvironment env) throws FileNotFoundException {
        if (input.endsWith(".json")) {
            LOGGER.info("Json input detected, instantiating from the cloud");
            return new DataSource<>(
                    env,
                    new BlobInputFormat(),
                    BasicTypeInfo.STRING_TYPE_INFO,
                    Utils.getCallLocationName()
            ).withParameters(AzureStorageIOConfig.confFromJson(input));
        }

        LOGGER.info("Instantiating locally");
        final Configuration parameters = new Configuration();
        parameters.setBoolean("recursive.file.enumeration", true);
        return new DataSource<>(
                env,
                new TextInputFormat(new Path(input)),
                BasicTypeInfo.STRING_TYPE_INFO,
                Utils.getCallLocationName()
        ).withParameters(parameters);
    }

    private ExecutionEnvironment getExecutionEnvironment() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final RestartStrategies.RestartStrategyConfiguration restartStrategy = RestartStrategies
                .failureRateRestart(
                        3,
                        Time.of(1, TimeUnit.MINUTES),
                        Time.of(30, TimeUnit.SECONDS)
                );
        env.setRestartStrategy(restartStrategy);
        return env;
    }

    private interface TermMapper extends FlatMapFunction<List<CleanUpText.SimpleTerm>, Tuple2<String, Integer>> {}

    private ImmutableMap<String, Long> instantiateWordDictionary(File pipelinePath) throws IOException {
        ImmutableMap.Builder<String, Long> builder = ImmutableMap.builder();
        long counter = 0;
        try (CSVParser parser = new CSVParser(new FileReader(new File(pipelinePath, WORD_DICT)), CSVFormat.TDF)) {
            for (CSVRecord record : parser) {
                builder.put(record.get(0), counter);
            }
        } catch (IOException e) {
            throw new IOException("Can't open word dictionary", e);
        }

        return builder.build();
    }

    private ImmutableMap<String, Long> instantiatePosTagDictionary(File pipelinePath) throws IOException {
        ImmutableMap.Builder<String, Long> builder = ImmutableMap.builder();
        long counter = 0;
        try (CSVParser parser = new CSVParser(new FileReader(new File(pipelinePath, POS_TAG_DICT)), CSVFormat.TDF)) {
            for (CSVRecord record : parser) {
                builder.put(record.get(0), counter);
            }
        } catch (IOException e) {
            throw new IOException("Can't open POS tag dictionary", e);
        }

        return builder.build();
    }

    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption("i", INPUT_CFG,
                        "specifies the input config or file(s)", "FILE/DIRECTORY",
                        CommandLine.Type.STRING, true, false, true)
                .withOption("o", OUTPUT_CFG,
                        "specifies the directory for the results and intermediate datasets", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true)
                .withOption("p", PIPELINE_PATH,
                        "file with configuration and resources", "CONFIG",
                        CommandLine.Type.STRING, true, false, true)
                .withOption("t", TENSORFLOW,
                        "should execute the second part of the pipeline");
    }

    public static void main(String[] args) throws Exception {
        FilterTextForRSDAE extractor = new FilterTextForRSDAE();

        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            String input = cmd.getOptionValue(INPUT_CFG, String.class);
            Configuration outputCfg = AzureStorageIOConfig.confFromJson(cmd.getOptionValue(OUTPUT_CFG, String.class));

            //noinspection ConstantConditions
            final File configFile = new File(cmd.getOptionValue(PIPELINE_PATH, String.class));

            if (cmd.hasOption(TENSORFLOW)) {
                extractor.start(input, outputCfg, configFile);
            } else {
                extractor.calculateStats(input, outputCfg, configFile);
            }
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }
}
