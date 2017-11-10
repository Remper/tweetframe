package eu.fbk.fm.tweetframe.pipeline.tweets;

import com.google.common.base.Stopwatch;
import eu.fbk.dkm.pikes.tintop.AnnotationPipeline;
import eu.fbk.dkm.pikes.tintop.annotators.Defaults;
import eu.fbk.dkm.pikes.tintop.server.Text2NafHandler;
import ixa.kaflib.KAFDocument;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Annotates text using the Pikes pipeline locally
 */
public class AnnotateLocal extends RichFlatMapFunction<String, KAFDocument> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AnnotateLocal.class);
    private static final long serialVersionUID = 1L;

    private String workingFolder;
    private AnnotationPipeline pipeline;

    public AnnotateLocal(File workingFolder) throws Exception {
        this.workingFolder = workingFolder.getPath();
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        String modelsFolder = workingFolder + File.separator + "models" + File.separator;
        Properties properties = new Properties();
        properties.setProperty("naf_filter_wordnet_path", workingFolder + File.separator + Defaults.WN_DICT);
        properties.setProperty("predicate_matrix", workingFolder + File.separator + Defaults.PREDICATE_MATRIX);
        properties.setProperty("on_frequencies", workingFolder + File.separator + Defaults.ON_FREQUENCIES);
        String ukbFolder = workingFolder + File.separator + Defaults.UKB_FOLDER;
        properties.setProperty("stanford.ukb.folder", workingFolder + File.separator + Defaults.UKB_FOLDER);
        properties.setProperty("stanford.ukb.model", ukbFolder + File.separator + Defaults.UKB_MODEL);
        properties.setProperty("stanford.ukb.dict", ukbFolder + File.separator + Defaults.UKB_DICT);
        properties.setProperty("stanford.ukb.instances", "1");
        properties.setProperty("stanford.ukb.restarts", "0");
        pipeline = new AnnotationPipeline(new File(workingFolder, "config-pikes.prop"), properties);
        pipeline.loadModels();
    }

    @Override
    public void flatMap(String value, Collector<KAFDocument> out) throws Exception {
        KAFDocument document = pipeline.parseFromNAF(Text2NafHandler.text2naf(value, new HashMap<>()));

        if (document == null) {
            getRuntimeContext().getIntCounter("ERROR").add(1);
            return;
        }

        out.collect(document);
    }
}
