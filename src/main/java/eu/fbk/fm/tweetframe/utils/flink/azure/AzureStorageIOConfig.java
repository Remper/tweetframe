package eu.fbk.fm.tweetframe.utils.flink.azure;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.flink.configuration.Configuration;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Serializable;
import java.util.UUID;

public class AzureStorageIOConfig implements Serializable {

    private static final int RECORDS_TO_FLUSH = 1000;
    private static final int RECORDS_TO_BREAK = 1000000;
    private static final String DEFAULT_BLOB_PREFIX = "output";

    public static final String AZURE_CONNECTION = "azure.connection";
    public static final String AZURE_CONTAINER_NAME = "azure.container.name";
    public static final String AZURE_BLOB_PREFIX = "azure.blob.prefix";
    public static final String AZURE_BLOB_SUFFIX = "azure.blob.suffix";
    public static final String AZURE_BLOB_BREAK = "azure.blob.break";
    public static final String AZURE_BLOB_FLUSH = "azure.blob.flush";
    public static final String AZURE_BLOB_ENABLE_COMPRESSION = "azure.blob.enable-compression";

    public String connectionString;
    public String containerName;
    public String blobPrefix;
    public String blobSuffix;
    public int blobBreak;
    public int blobFlush;
    public boolean enableCompression;

    public AzureStorageIOConfig(Configuration parameters) {
        connectionString = parameters.getString(AZURE_CONNECTION, "");
        containerName = parameters.getString(AZURE_CONTAINER_NAME, UUID.randomUUID().toString());
        enableCompression = parameters.getBoolean(AZURE_BLOB_ENABLE_COMPRESSION, false);
        blobPrefix = parameters.getString(AZURE_BLOB_PREFIX, DEFAULT_BLOB_PREFIX);
        blobSuffix = parameters.getString(AZURE_BLOB_SUFFIX, enableCompression ? ".gz" : ".txt");
        blobBreak = parameters.getInteger(AZURE_BLOB_BREAK, RECORDS_TO_BREAK);
        blobFlush = parameters.getInteger(AZURE_BLOB_FLUSH, RECORDS_TO_FLUSH);
    }

    public static Configuration confFromJson(String filename) throws FileNotFoundException {
        Configuration parameters = new Configuration();
        new Gson()
                .fromJson(new FileReader(filename), JsonObject.class)
                .entrySet()
                .forEach(entry -> {
                    JsonPrimitive primitive = entry.getValue().getAsJsonPrimitive();
                    if (primitive.isBoolean()) {
                        parameters.setBoolean(entry.getKey(), primitive.getAsBoolean());
                        return;
                    }

                    parameters.setString(entry.getKey(), primitive.getAsString());
                });
        return parameters;
    }

    public static AzureStorageIOConfig fromJson(String filename) throws FileNotFoundException {
        return new AzureStorageIOConfig(confFromJson(filename));
    }
}
