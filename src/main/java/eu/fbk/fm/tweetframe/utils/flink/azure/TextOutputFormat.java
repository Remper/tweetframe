package eu.fbk.fm.tweetframe.utils.flink.azure;

import java.io.IOException;

public class TextOutputFormat<IT> extends BlobOutputFormat<IT> {
    @Override
    public void writeRecord(IT record) throws IOException {
        StringBuilder sb = new StringBuilder();
        if (!isFirst) {
            sb.append('\n');
        }
        sb.append(record.toString());

        this.writeRecord(sb.toString().getBytes());
    }
}
