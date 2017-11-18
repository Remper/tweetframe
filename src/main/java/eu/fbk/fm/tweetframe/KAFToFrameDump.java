package eu.fbk.fm.tweetframe;

import eu.fbk.fm.tweetframe.pipeline.text.FrameDataFromKAF;
import eu.fbk.utils.core.CommandLine;
import ixa.kaflib.KAFDocument;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Optional;

public class KAFToFrameDump {

    private static final Logger LOGGER = LoggerFactory.getLogger(KAFToFrameDump.class);

    private void start(File inputDirectory, File outputFile) throws Exception {
        FrameDataFromKAF pipeline = new FrameDataFromKAF();

        try (FileWriter writer = new FileWriter(outputFile)) {
            for (File inputFile : Optional.ofNullable(inputDirectory.listFiles()).orElse(new File[0])) {
                if (inputFile.isDirectory() || !inputFile.getName().endsWith(".xml")) {
                    continue;
                }

                Collector<String> collector = new Collector<String>() {
                    @Override
                    public void collect(String record) {
                        try {
                            writer.write('\n');
                            writer.write(record);
                        } catch (IOException e) {
                            LOGGER.error("Error while writing to file", e);
                        }
                    }

                    @Override
                    public void close() {

                    }
                };
                pipeline.flatMap(new Tuple2<>(KAFDocument.createFromFile(inputFile), 0), collector);
            }
        }
    }

    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption("i", "input",
                        "", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true)
                .withOption("o", "output",
                        "", "FILE",
                        CommandLine.Type.STRING, true, false, true);
    }

    public static void main(String[] args) throws Exception {
        KAFToFrameDump extractor = new KAFToFrameDump();

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
