package eu.fbk.fm.tweetframe;

import com.google.common.io.ByteStreams;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.ListBlobItem;
import eu.fbk.fm.tweetframe.utils.flink.azure.AzureStorageIOConfig;
import eu.fbk.utils.core.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class AzureStorageConsole {

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureStorageConsole.class);

    private CloudBlobContainer getContainerReference(AzureStorageIOConfig config) throws Exception {
        return CloudStorageAccount
                .parse(config.connectionString)
                .createCloudBlobClient()
                .getContainerReference(config.containerName);
    }

    private void listCloudFiles(String azureConfFile) throws Exception {
        AzureStorageIOConfig config = AzureStorageIOConfig.fromJson(azureConfFile);
        config.blobPrefix = "";
        CloudBlobContainer container = getContainerReference(config);
        if (!container.exists()) {
            LOGGER.info("Container doesn't exist");
            return;
        }

        HashMap<String, Long> sizes = new HashMap<>();
        HashMap<String, Integer> chunks = new HashMap<>();
        for (ListBlobItem item : container.listBlobs(config.blobPrefix)) {
            if (!(item instanceof CloudBlob)) {
                continue;
            }

            String fileName = ((CloudBlob) item).getName().replaceFirst("(-[0-9]+)*\\.[a-z]+$", "");
            long curSize = sizes.getOrDefault(fileName, 0L);
            curSize += ((CloudBlob) item).getProperties().getLength();
            sizes.put(fileName, curSize);
            chunks.put(fileName, chunks.getOrDefault(fileName, 0)+1);
        }

        for (String fileName : sizes.keySet()) {
            LOGGER.info(String.format(
                    "File %s, chunks %d, total size: %s",
                    fileName,
                    chunks.get(fileName),
                    produceFileLength(sizes.get(fileName))
            ));
        }
    }

    private void assembleCloudFile(String azureConfFile, File outputFile) throws Exception {
        AzureStorageIOConfig config = AzureStorageIOConfig.fromJson(azureConfFile);
        CloudBlobContainer container = getContainerReference(config);
        if (!container.exists()) {
            LOGGER.info("Container doesn't exist");
            return;
        }

        try (OutputStream output = new GZIPOutputStream(new FileOutputStream(outputFile))) {
            for (ListBlobItem item : container.listBlobs(config.blobPrefix)) {
                if (!(item instanceof CloudBlob)) {
                    continue;
                }
                LOGGER.info(String.format(
                        "Dumping file %s (%s)",
                        ((CloudBlob) item).getName(),
                        produceFileLength(((CloudBlob) item).getProperties().getLength()))
                );

                InputStream stream = ((CloudBlob) item).openInputStream();
                if (config.enableCompression) {
                    stream = new GZIPInputStream(stream);
                }

                ByteStreams.copy(stream, output);
            }
        }
    }

    private static String produceFileLength(long bytes) {
        String suffix = "KB";
        long divider = 1024;

        if (bytes > 1024 * 1024 * 1024) {
            suffix = "GB";
            divider = 1024 * 1024 * 1024;
        } else if (bytes > 1024 * 1024) {
            suffix = "MB";
            divider = 1024 * 1024;
        }
        return String.format("%.2f%s", (double) bytes / divider, suffix);
    }

    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption("a", "azure-conf",
                        "", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true)
                .withOption("o", "output",
                        "", "FILE",
                        CommandLine.Type.STRING, true, false, true)
                .withOption("m", "mode",
                        "", "MODE",
                        CommandLine.Type.STRING, true, false, false);
    }

    public static void main(String[] args) throws Exception {
        AzureStorageConsole extractor = new AzureStorageConsole();

        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            //noinspection ConstantConditions
            final String azureConfFile = cmd.getOptionValue("azure-conf", String.class);

            //noinspection ConstantConditions
            final File output = new File(cmd.getOptionValue("output", String.class));

            String mode = cmd.getOptionValue("mode", String.class);
            if (mode == null) {
                mode = "list";
            }

            switch (mode) {
                default:
                case "list":
                    extractor.listCloudFiles(azureConfFile);
                    break;
                case "assemble":
                    extractor.assembleCloudFile(azureConfFile, output);
                    break;
            }
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }
}
