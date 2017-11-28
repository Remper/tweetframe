package eu.fbk.fm.tweetframe.utils.flink.azure;

import org.tensorflow.example.*;

import java.io.IOException;

public class RawOutputFormat extends BlobOutputFormat<byte[]> {
    @Override
    public void writeRecord(byte[] record) throws IOException {
        super.writeRecord(record);
    }
}
