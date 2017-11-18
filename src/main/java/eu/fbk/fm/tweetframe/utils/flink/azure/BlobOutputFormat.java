package eu.fbk.fm.tweetframe.utils.flink.azure;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.zip.GZIPOutputStream;

/**
 * Interprets input as a string and writes each record on the separate line to Azure
 */
public class BlobOutputFormat<IT> extends RichOutputFormat<IT> {

    protected AzureStorageIOConfig config;

    private transient CloudBlobContainer container;
    protected transient CloudBlockBlob blob;
    protected transient OutputStream stream;
    private boolean isFirst;
    private int recordsWritten;

    @Override
    public void configure(Configuration parameters) {
        config = new AzureStorageIOConfig(parameters);
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        config.blobPrefix += "-" + taskNumber + "-" + numTasks;
        isFirst = true;
        recordsWritten = 0;
        try {
            container = CloudStorageAccount
                    .parse(config.connectionString)
                    .createCloudBlobClient()
                    .getContainerReference(config.containerName);
            container.createIfNotExists();
            openStream();
        } catch (Exception e) {
            throw new IOException("Can't connect to Azure Cloud", e);
        }
    }

    private void openStream() throws URISyntaxException, StorageException, IOException {
        String prefix = config.blobPrefix;
        int part = config.blobBreak == 0 ? 0 : recordsWritten / config.blobBreak;
        if (part > 0) {
            prefix += "-" + part;
        }
        blob = container.getBlockBlobReference(prefix + config.blobSuffix);
        stream = blob.openOutputStream(AccessCondition.generateIfNotExistsCondition(), null, null);
        if (config.enableCompression) {
            stream = new GZIPOutputStream(stream);
        }
    }

    @Override
    public void writeRecord(IT record) throws IOException {
        if (recordsWritten % config.blobFlush == 0 && recordsWritten > 0) {
            stream.flush();
        }

        if (config.blobBreak > 0 && recordsWritten % config.blobBreak == 0 && recordsWritten > 0) {
            try {
                stream.close();
                openStream();
                isFirst = true;
            } catch (Exception e) {
                throw new IOException("Can't reopen stream after a break");
            }
        }

        if (!isFirst) {
            stream.write(new byte[] {'\n'});
        }
        isFirst = false;
        stream.write(record.toString().getBytes());
        recordsWritten++;
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }
}
