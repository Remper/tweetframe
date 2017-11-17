package eu.fbk.fm.tweetframe.utils.flink.azure;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.*;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;

public class BlobInputFormat extends RichInputFormat<String, BlobInputSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(BlobInputFormat.class);

    protected AzureStorageIOConfig config;

    private CloudBlobContainer container;
    private BufferedReader reader;
    private String curLine;
    private boolean reachedEnd;

    @Override
    public void configure(Configuration parameters) {
        config = new AzureStorageIOConfig(parameters);
    }

    private void initContainer() throws IOException {
        if (container == null) {
            try {
                container = CloudStorageAccount
                        .parse(config.connectionString)
                        .createCloudBlobClient()
                        .getContainerReference(config.containerName);
            } catch (Exception e) {
                throw new IOException("Can't initialise container", e);
            }
        }
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return null;
    }

    @Override
    public BlobInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        ArrayList<BlobInputSplit> splits = new ArrayList<>();
        int counter = 0;

        initContainer();
        for (ListBlobItem blobItem : container.listBlobs(config.blobPrefix, true)) {
            if (!(blobItem instanceof CloudBlockBlob)) {
                continue;
            }
            splits.add(new BlobInputSplit(counter++, ((CloudBlockBlob) blobItem).getName()));
        }

        return splits.toArray(new BlobInputSplit[splits.size()]);
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(BlobInputSplit[] inputSplits) {
        return new InputSplitAssigner() {
            int nextSplit = 0;

            @Override
            public synchronized InputSplit getNextInputSplit(String host, int taskId) {
                if (nextSplit >= inputSplits.length) {
                    return null;
                }

                return inputSplits[nextSplit++];
            }
        };
    }

    @Override
    public void open(BlobInputSplit split) throws IOException {
        initContainer();
        try {
            CloudBlockBlob blob = container.getBlockBlobReference(split.getReference());
            reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(blob.openInputStream())));
            reachedEnd = false;
        } catch (Exception e) {
            throw new IOException("Can't open split: "+split.getReference(), e);
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return reachedEnd || (reachedEnd = (curLine == null && (curLine = reader.readLine()) == null));
    }

    @Override
    public String nextRecord(String reuse) throws IOException {
        if (reachedEnd() || curLine == null) {
            throw new IOException("The end was reached");
        }
        String nextRecord = curLine;
        curLine = null;
        return nextRecord;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
