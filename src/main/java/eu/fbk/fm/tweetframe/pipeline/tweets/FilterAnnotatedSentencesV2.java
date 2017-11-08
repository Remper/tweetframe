package eu.fbk.fm.tweetframe.pipeline.tweets;

import eu.fbk.fm.tweetframe.utils.flink.JsonObjectProcessor;
import ixa.kaflib.KAFDocument;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * Second version that tries to find exact triples that could be found in the dataset
 */
public class FilterAnnotatedSentencesV2 extends RichFlatMapFunction<KAFDocument, Tuple3<String, Integer, String>> implements JsonObjectProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterAnnotatedSentencesV2.class);
    private static final long serialVersionUID = 1L;

    public static final int FILTER_OUT = -1;
    public static final int NORMAL_PRIORITY = 2;
    public static final int HIGH_PRIORITY = 3;

    private final String dataFolder;

    private transient RelationRepository relations;
    private transient HashMap<String, String> synsets;

    public FilterAnnotatedSentencesV2(String dataFolder) {
        this.dataFolder = dataFolder;
    }

    @Override
    public void open(Configuration configuration) throws IOException {
        restoreData(dataFolder);
    }

    @Override
    public void flatMap(KAFDocument document, Collector<Tuple3<String, Integer, String>> out) throws Exception {
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

        StringBuilder extractedMatches = new StringBuilder();
        for (String relation : rels) {
            HashMap<String, HashMap<String, Relation>> relationObjects = relations.get(relation);
            for (String object : objects) {
                HashMap<String, Relation> relationSubjects = relationObjects.get(synsets.get(object));
                if (relationSubjects == null) {
                    continue;
                }

                boolean match = false;
                for (String subject : objects) {
                    Relation resolvedRelation = relationSubjects.get(synsets.get(subject));
                    if (resolvedRelation == null) {
                        continue;
                    }

                    //Print complete match
                    match = true;
                    priority = HIGH_PRIORITY;
                    printMatch(resolvedRelation, extractedMatches);
                }

                //Print partial match
                if (!match) {
                    priority = priority > NORMAL_PRIORITY ? priority : NORMAL_PRIORITY;
                    printMatch(new Relation(relation, object, "-"), extractedMatches);
                }
            }
        }

        if (priority > FILTER_OUT) {
            getRuntimeContext().getIntCounter("PRIORITY_" + priority).add(1);
            out.collect(new Tuple3<>(document.getRawText(), priority, extractedMatches.toString()));
        } else {
            getRuntimeContext().getIntCounter("FILTER_OUT").add(1);
        }
    }

    private void printMatch(Relation match, StringBuilder builder) {
        if (builder.length() != 0) {
            builder.append(",");
        }
        builder.append("<");
        builder.append(synsets.get(match.object));
        builder.append(",");
        builder.append(synsets.get(match.synset));
        builder.append(",");
        builder.append(synsets.get(match.subject));
        builder.append(">");
    }

    private void restoreData(String directory) throws IOException {
        relations = new RelationRepository();
        synsets = new HashMap<>();

        File relationsFile = new File(directory, "raw_relations.tsv");
        File synsetsFile = new File(directory, "synsets.tsv");

        try (CSVParser synsetParser = new CSVParser(new FileReader(synsetsFile), CSVFormat.TDF)) {
            synsetParser.forEach(record -> synsets.put(record.get(0), record.get(1)));
        }

        try (CSVParser synsetParser = new CSVParser(new FileReader(relationsFile), CSVFormat.TDF)) {
            synsetParser.forEach(record -> relations.add(record.get(1), record.get(0), record.get(2)));
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
