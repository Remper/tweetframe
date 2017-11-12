package eu.fbk.fm.tweetframe.utils.flink.azure;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobOutputStream;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

/**
 * Interprets input as a string and writes each record on the separate line to Azure
 */
public class BlobOutputFormat<IT> extends RichOutputFormat<IT> {

    private static final int RECORDS_TO_FLUSH = 1000;
    private static final int RECORDS_TO_BREAK = 1000000;

    public static final String AZURE_CONNECTION = "azure.connection";
    public static final String AZURE_CONTAINER_NAME = "azure.container.name";
    public static final String AZURE_BLOB_PREFIX = "azure.blob.prefix";
    public static final String AZURE_BLOB_SUFFIX = "azure.blob.suffix";
    public static final String AZURE_BLOB_ENABLE_COMPRESSION = "azure.blob.enable-compression";

    private String connectionString;
    private String containerName;
    private String blobPrefix;
    private String blobSuffix;
    private boolean enableCompression;

    private transient CloudBlobClient connection;
    private transient CloudBlobContainer container;
    private transient CloudBlockBlob blob;
    private transient OutputStream stream;
    private boolean isFirst;
    private int recordsWritten;

    @Override
    public void configure(Configuration parameters) {
        connectionString = parameters.getString(AZURE_CONNECTION, "");
        containerName = parameters.getString(AZURE_CONTAINER_NAME, UUID.randomUUID().toString());
        blobPrefix = parameters.getString(AZURE_BLOB_PREFIX, "output");
        blobSuffix = parameters.getString(AZURE_BLOB_SUFFIX, null);
        enableCompression = parameters.getBoolean(AZURE_BLOB_ENABLE_COMPRESSION, false);
        if (blobSuffix == null) {
            blobSuffix = enableCompression ? ".gz" : ".txt";
        }
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        blobPrefix += "-" + taskNumber + "-" + numTasks;
        isFirst = true;
        recordsWritten = 0;
        try {
            CloudStorageAccount account = CloudStorageAccount.parse(connectionString);
            connection = account.createCloudBlobClient();
            container = connection.getContainerReference(containerName);
            container.createIfNotExists();
            openStream();
        } catch (Exception e) {
            throw new IOException("Can't connect to Azure Cloud", e);
        }
    }

    private void openStream() throws URISyntaxException, StorageException, IOException {
        String prefix = blobPrefix;
        int part = RECORDS_TO_BREAK == 0 ? 0 : recordsWritten / RECORDS_TO_BREAK;
        if (part > 0) {
            prefix += "-" + part;
        }
        blob = container.getBlockBlobReference(prefix + blobSuffix);
        stream = blob.openOutputStream(AccessCondition.generateIfNotExistsCondition(), null, null);
        if (enableCompression) {
            stream = new GZIPOutputStream(stream);
        }
    }

    @Override
    public void writeRecord(IT record) throws IOException {
        if (recordsWritten % RECORDS_TO_FLUSH == 0 && recordsWritten > 0) {
            stream.flush();
        }

        if (recordsWritten % RECORDS_TO_BREAK == 0 && recordsWritten > 0) {
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
