package eu.fbk.fm.tweetframe.pipeline.text;

import eu.fbk.dkm.pikes.tintop.AnnotationPipeline;
import eu.fbk.dkm.pikes.tintop.server.Text2NafHandler;
import ixa.kaflib.KAFDocument;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Properties;

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
        properties.setProperty("naf_filter_wordnet_path", workingFolder + File.separator + "wordnet" + File.separator);
        properties.setProperty("predicate_matrix", modelsFolder + "PredicateMatrix.txt");
        properties.setProperty("on_frequencies", modelsFolder + "on-frequencies.tsv");
        String ukbFolder = workingFolder + File.separator + "ukb" + File.separator;
        String ukbModelFolder = ukbFolder + "models" + File.separator;
        properties.setProperty("stanford.ukb.folder", ukbFolder);
        properties.setProperty("stanford.ukb.model", ukbModelFolder + "wnet30_wnet30g_rels.bin");
        properties.setProperty("stanford.ukb.dict", ukbModelFolder + "wnet30_dict.txt");
        properties.setProperty("stanford.semafor.model_dir", modelsFolder + "semafor" + File.separator);
        properties.setProperty("stanford.conll_parse.model", modelsFolder + "anna_parse.model");
        properties.setProperty("stanford.mate.model", modelsFolder + "mate.model");
        properties.setProperty("stanford.mate.model_be", modelsFolder + "mate_be.model");
        pipeline = new AnnotationPipeline(new File(workingFolder, "config-pikes.prop"), properties);
    }

    @Override
    public void flatMap(String value, Collector<KAFDocument> out) throws Exception {
        KAFDocument document = pipeline.parseFromNAF(Text2NafHandler.text2naf(value, new HashMap<>()));

        if (document == null) {
            getRuntimeContext().getIntCounter("ERROR").add(1);
            return;
        }

        getRuntimeContext().getIntCounter("ANNOTATED_SENTENCES").add(1);
        out.collect(document);
    }
}
