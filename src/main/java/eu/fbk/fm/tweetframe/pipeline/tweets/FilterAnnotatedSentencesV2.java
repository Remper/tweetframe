package eu.fbk.fm.tweetframe.pipeline.tweets;

import eu.fbk.fm.tweetframe.utils.flink.JsonObjectProcessor;
import ixa.kaflib.KAFDocument;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Second version that tries to find exact triples that could be found in the dataset
 */
public class FilterAnnotatedSentencesV2 extends RichFlatMapFunction<KAFDocument, Tuple2<KAFDocument, Integer>> implements JsonObjectProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterAnnotatedSentencesV2.class);
    private static final long serialVersionUID = 1L;

    public static final int FILTER_OUT = -1;
    public static final int NORMAL_PRIORITY = 2;
    public static final int HIGH_PRIORITY = 3;

    private final File dataFolder;

    private transient RelationRepository relations;
    private transient HashMap<String, String> synsets;

    public FilterAnnotatedSentencesV2(File dataFolder) {
        this.dataFolder = dataFolder;
    }

    @Override
    public void open(Configuration configuration) throws IOException {
        restoreData(dataFolder);
    }

    private void restoreData(File directory) throws IOException {
        relations = new RelationRepository();
        synsets = new HashMap<>();

        File relationsFile = new File(directory, "raw_relations.tsv");
        File synsetsFile = new File(directory, "synsets.tsv");

        try (CSVParser synsetParser = new CSVParser(new FileReader(synsetsFile), CSVFormat.TDF)) {
            synsetParser.forEach(record -> synsets.put(record.get(0), record.get(1)));
        }

        try (CSVParser synsetParser = new CSVParser(new FileReader(relationsFile), CSVFormat.TDF)) {
            synsetParser.forEach(record -> {
                relations.add(record.get(1), record.get(0), record.get(2));
                relations.add(record.get(2), record.get(0), record.get(1));
            });
        }
    }

    @Override
    public void flatMap(KAFDocument document, Collector<Tuple2<KAFDocument, Integer>> collector) throws Exception {

        int priority = -1;
        HashSet<String> objects = new HashSet<>();
        List<String> rels = new LinkedList<>();
        document.getTerms().forEach(entity -> {
            entity.getExternalRefs().forEach(ref -> {
                String refValue = ref.getReference();

                if (!refValue.matches("[0-9]{8}-[a-z]")) {
                    return;
                }

                if (!synsets.containsKey(refValue)) {
                    return;
                }

                if (relations.hasRelation(refValue)) {
                    rels.add(refValue);
                    return;
                }

                objects.add(refValue);
            });
        });

        for (String relation : rels) {
            HashMap<String, HashMap<String, Relation>> relationObjects = relations.get(relation);
            for (String object : objects) {
                HashMap<String, Relation> relationSubjects = relationObjects.get(object);
                if (relationSubjects == null) {
                    continue;
                }

                priority = priority > NORMAL_PRIORITY ? priority : NORMAL_PRIORITY;
                for (String subject : objects) {
                    Relation resolvedRelation = relationSubjects.get(subject);
                    if (resolvedRelation == null) {
                        continue;
                    }

                    priority = HIGH_PRIORITY;
                    break;
                }
            }
        }

        getRuntimeContext().getIntCounter("PRIORITY_" + priority).add(1);
        if (priority > FILTER_OUT) {
            collector.collect(new Tuple2<>(document, priority));
        }
    }

    private static class RelationRepository extends HashMap<String, HashMap<String, HashMap<String, Relation>>>  {
        public Relation get(String object, String relation, String subject) {
            return Optional.of(this)
                    .map(ele -> ele.get(relation))
                    .map(ele -> ele.get(object))
                    .map(ele -> ele.get(subject))
                    .orElse(null);
        }

        public boolean hasRelation(String relation) {
            return containsKey(relation);
        }

        public void add(String object, String relation, String subject) {
            HashMap<String, HashMap<String, Relation>> relationObjects = getOrDefault(relation, new HashMap<>());
            putIfAbsent(relation, relationObjects);

            HashMap<String, Relation> relationSubjects = relationObjects.getOrDefault(object, new HashMap<>());
            relationObjects.putIfAbsent(object, relationSubjects);

            Relation resolvedRelation = relationSubjects.getOrDefault(subject, new Relation(relation, object, subject));
            relationSubjects.putIfAbsent(subject, resolvedRelation);

            resolvedRelation.freq += 1;
        }
    }

    private static class Relation {
        final String synset;
        final String object;
        final String subject;
        int freq = 0;

        public Relation(String synset, String object, String subject) {
            this.synset = synset;
            this.object = object;
            this.subject = subject;
        }
    }
}
