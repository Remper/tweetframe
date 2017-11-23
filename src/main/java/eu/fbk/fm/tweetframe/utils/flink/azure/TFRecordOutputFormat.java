package eu.fbk.fm.tweetframe.utils.flink.azure;

import org.tensorflow.example.*;

import java.io.IOException;

public class TFRecordOutputFormat extends BlobOutputFormat<Example> {
    @Override
    public void writeRecord(Example it) throws IOException {
        this.writeRecord(it.toByteArray());
    }
}
