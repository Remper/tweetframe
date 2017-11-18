package eu.fbk.fm.tweetframe.utils.flink.azure;

import org.apache.flink.core.io.InputSplit;

public class BlobInputSplit implements InputSplit {

    int num;
    String reference;

    public BlobInputSplit(int num, String reference) {
        this.num = num;
        this.reference = reference;
    }

    @Override
    public int getSplitNumber() {
        return num;
    }

    public String getReference() {
        return reference;
    }
}
