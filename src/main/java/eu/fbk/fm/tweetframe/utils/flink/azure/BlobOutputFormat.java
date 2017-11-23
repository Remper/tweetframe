package eu.fbk.fm.tweetframe.utils.flink.azure;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.zip.GZIPOutputStream;

/**
 * Interprets input as a string and writes each record on the separate line to Azure
 */
public abstract class BlobOutputFormat<IT> extends RichOutputFormat<IT> {

    protected AzureStorageIOConfig config;

    private transient CloudBlobContainer container;
    protected transient CloudBlockBlob blob;
    protected transient OutputStream stream;
    protected boolean isFirst = true;
    private int recordsWritten = 0;

    @Override
    public void configure(Configuration parameters) {
        config = new AzureStorageIOConfig(parameters);
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        if (numTasks > 1) {
            config.blobPrefix += "-" + taskNumber + "-" + numTasks;
        }
        recordsWritten = 0;
        try {
            container = getContainerReference();
            container.createIfNotExists();
            openStream();
        } catch (Exception e) {
            throw new IOException("Can't connect to Azure Cloud", e);
        }
    }

    private void openStream() throws URISyntaxException, StorageException, IOException {
        isFirst = true;
        blob = container.getBlockBlobReference(getFilename());
        stream = blob.openOutputStream(AccessCondition.generateIfNotExistsCondition(), null, null);
        if (config.enableCompression) {
            stream = new GZIPOutputStream(stream);
        }
    }

    private CloudBlobContainer getContainerReference() throws URISyntaxException, InvalidKeyException, StorageException {
        CloudBlobContainer container = CloudStorageAccount
                .parse(config.connectionString)
                .createCloudBlobClient()
                .getContainerReference(config.containerName);
        container.createIfNotExists();
        return container;
    }

    private String getFilename() {
        String prefix = config.blobPrefix;
        int part = config.blobBreak == 0 ? 0 : recordsWritten / config.blobBreak;
        if (part > 0) {
            prefix += "-" + part;
        }
        return prefix + config.blobSuffix;
    }

    public boolean exists() throws IOException {
        try {
            return getContainerReference()
                    .listBlobs(config.blobPrefix)
                    .iterator()
                    .hasNext();
        } catch (Exception e) {
            String errMessage = String.format("Something went wrong while checking existence of blobs with prefix %s", config.blobPrefix);
            throw new IOException(errMessage, e);
        }
    }

    public static boolean exists(Configuration configuration) throws IOException {
        BlobOutputFormat format = new BlobOutputFormat() {
            @Override
            public void writeRecord(Object record) throws IOException {

            }
        };
        format.configure(configuration);
        return format.exists();
    }

    protected void writeRecord(byte[] record) throws IOException {
        isFirst = false;
        stream.write(record);
        recordsWritten++;

        if (recordsWritten % config.blobFlush == 0) {
            stream.flush();
        }

        if (config.blobBreak > 0 && recordsWritten % config.blobBreak == 0) {
            try {
                stream.close();
                openStream();
            } catch (Exception e) {
                throw new IOException(String.format("Can't open stream (%d records written)", recordsWritten), e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }


}
