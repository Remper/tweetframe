package eu.fbk.fm.tweetframe.utils.flink;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.fs.Path;

import java.io.IOException;

/**
 * A TsvOutputFormat that also controls the maximum size of the output
 */
public class LimitedSizeTsvOutputFormat<T extends Tuple> extends RobustTsvOutputFormat<T> {
    private static final long serialVersionUID = 1L;

    private int currentRecord = 0;
    private int taskNumber = 0;
    private int numTasks = 0;
    private int limit = 1000;

    public LimitedSizeTsvOutputFormat(Path outputPath) {
        super(outputPath);
    }

    public LimitedSizeTsvOutputFormat(Path outputPath, boolean enableGzip) {
        super(outputPath, enableGzip);
    }

    public LimitedSizeTsvOutputFormat<T> limit(int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        this.taskNumber = taskNumber;
        this.numTasks = numTasks;
        super.open(taskNumber, numTasks);
    }

    public void open() throws IOException {
        this.open(this.taskNumber, this.numTasks);
    }

    @Override
    protected String getDirectoryFileName(int taskNumber) {
        return Integer.toString(taskNumber + 1) + "-" + Integer.toString(currentRecord / this.limit);
    }

    @Override
    public void writeRecord(T element) throws IOException {
        currentRecord++;
        if (currentRecord % this.limit == 0) {
            this.close();
            this.open();
        }
        super.writeRecord(element);
    }
}
