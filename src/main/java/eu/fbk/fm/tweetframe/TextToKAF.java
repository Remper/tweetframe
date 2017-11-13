package eu.fbk.fm.tweetframe;

import eu.fbk.fm.tweetframe.pipeline.text.AnnotateServer;
import eu.fbk.utils.core.CommandLine;
import ixa.kaflib.KAFDocument;
import org.apache.commons.io.IOUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Optional;

public class TextToKAF {

    private static final Logger LOGGER = LoggerFactory.getLogger(TextToKAF.class);

    private void start(File inputDirectory, File outputDirectory) throws Exception {
        AnnotateServer server = new AnnotateServer("http://localhost:8011/text2naf");
        server.open(new Configuration());

        for (File inputFile : Optional.ofNullable(inputDirectory.listFiles()).orElse(new File[0])) {
            if (inputFile.isDirectory()) {
                continue;
            }

            int[] outputNum = {0};

            String[] input = IOUtils.toString(new FileReader(inputFile)).split("\n");
            Collector<KAFDocument> collector = new Collector<KAFDocument>() {
                @Override
                public void collect(KAFDocument record) {
                    String filename = inputFile.getName();
                    int lastIdx = filename.lastIndexOf('.');
                    if (lastIdx > 0) {
                        filename = filename.substring(0, lastIdx) + "-" + outputNum[0] + ".xml";
                    } else {
                        filename = filename + "-" + outputNum[0];
                    }

                    record.save(new File(outputDirectory, filename));
                    outputNum[0]++;
                }

                @Override
                public void close() {

                }
            };
            Arrays.stream(input).forEach(s -> {
                try {
                    server.flatMap(s, collector);
                } catch (Exception e) {
                    LOGGER.error("Something happened while annotating", e);
                }
            });
        }
    }

    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption("i", "input",
                        "", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true)
                .withOption("o", "output",
                        "", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true);
    }

    public static void main(String[] args) throws Exception {
        TextToKAF extractor = new TextToKAF();

        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            //noinspection ConstantConditions
            final File input = new File(cmd.getOptionValue("input", String.class));

            //noinspection ConstantConditions
            final File output = new File(cmd.getOptionValue("output", String.class));

            extractor.start(input, output);
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }
}
