package eu.fbk.fm.tweetframe.pipeline.tweets;

import com.google.common.collect.Sets;
import eu.fbk.fm.tweetframe.utils.flink.JsonObjectProcessor;
import ixa.kaflib.Entity;
import ixa.kaflib.KAFDocument;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.jooq.tools.csv.CSVReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.function.Consumer;

/**
 * Created by remper on 03/11/2017.
 */
public class Annotate extends RichFlatMapFunction<Tuple1<String>, Tuple2<String, Integer>> implements JsonObjectProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(Annotate.class);
    private static final long serialVersionUID = 1L;

    public static final int FILTER_OUT = -1;
    public static final int NORMAL_PRIORITY = 0;
    public static final int THIRD_PRIORITY = 1;
    public static final int SECOND_PRIORITY = 2;
    public static final int HIGH_PRIORITY = 3;

    private final String requestURI;
    private final String dataFolder;

    private transient CloseableHttpClient httpclient;
    private transient HashMap<String, Relation> relations;
    private transient Set<String> synsets;

    public Annotate(String requestURI, String dataFolder) {
        this.requestURI = requestURI;
        this.dataFolder = dataFolder;
    }

    @Override
    public void open(Configuration configuration) throws IOException {
        httpclient = HttpClients.createDefault();
        restoreData(dataFolder);
    }

    @Override
    public void flatMap(Tuple1<String> value, Collector<Tuple2<String, Integer>> out) throws Exception {
        HttpPost request = new HttpPost(requestURI);
        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("text", value.f0));
        request.setEntity(new UrlEncodedFormEntity(params));

        KAFDocument document;
        try (CloseableHttpResponse response = httpclient.execute(request)) {
            HttpEntity entity = response.getEntity();
            document = KAFDocument.createFromStream(new InputStreamReader(entity.getContent()));
            EntityUtils.consume(entity);
        }

        if (document == null) {
            getRuntimeContext().getIntCounter("ERROR").add(1);
            return;
        }

        int priority = FILTER_OUT;
        HashSet<String> objects = new HashSet<>();
        List<Relation> rels = new LinkedList<>();
        document.getTerms().forEach(entity -> {
            entity.getExternalRefs().forEach(ref -> {
                String refValue = ref.getReference();

                if (!refValue.matches("[0-9]{8}-[a-z]")) {
                    return;
                }

                if (!synsets.contains(refValue)) {
                    return;
                }

                if (relations.containsKey(refValue)) {
                    rels.add(relations.get(refValue));
                    return;
                }

                objects.add(refValue);
            });
        });

        int matches = rels.size() + objects.size();
        if (matches == 0) {
            getRuntimeContext().getIntCounter("FILTER_OUT").add(1);
            return;
        }

        priority = NORMAL_PRIORITY;
        if (matches > 1) {
            priority = THIRD_PRIORITY;
        }

        for (Relation relation : rels) {
            int filledObjRoles = Sets.intersection(relation.objects, objects).size();
            int filledSubjRoles = Sets.intersection(relation.subjects, objects).size();
            if (filledObjRoles > 0 && filledSubjRoles > 0) {
                priority = HIGH_PRIORITY;
                break;
            } else if (filledObjRoles + filledSubjRoles > 0) {
                priority = SECOND_PRIORITY;
            }
        }

        getRuntimeContext().getIntCounter("PRIORITY_"+priority).add(1);
        out.collect(new Tuple2<>(value.f0, priority));
    }

    @Override
    public void close() throws Exception {
        httpclient.close();
    }

    private void restoreData(String directory) throws IOException {
        relations = new HashMap<>();
        synsets = new HashSet<>();

        File relationsFile = new File(directory, "relations.tsv");
        File synsetsFile = new File(directory, "synsets.tsv");

        try (CSVParser relationsParser = new CSVParser(new FileReader(relationsFile), CSVFormat.TDF)) {
            relationsParser.forEach(record -> relations.put(record.get(1), Relation.processRecord(record)));
        }

        try (CSVParser synsetParser = new CSVParser(new FileReader(synsetsFile), CSVFormat.TDF)) {
            synsetParser.forEach(record -> synsets.add(record.get(0)));
        }
    }

    private static class Relation {
        final String synset;
        final HashSet<String> objects;
        final HashSet<String> subjects;

        public Relation(String synset, HashSet<String> objects, HashSet<String> subjects) {
            this.synset = synset;
            this.objects = objects;
            this.subjects = subjects;
        }

        public static HashSet<String> extractSynsets(String synsetString) {
            HashSet<String> result = new HashSet<>();
            result.addAll(Arrays.asList(synsetString.split(",")));
            return result;
        }

        public static Relation processRecord(CSVRecord record) {
            return new Relation(
                    record.get(1),
                    extractSynsets(record.get(2)),
                    extractSynsets(record.get(3))
            );
        }
    }
}
