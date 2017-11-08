package eu.fbk.fm.tweetframe.pipeline.tweets;

import com.google.common.collect.Sets;
import eu.fbk.fm.tweetframe.utils.flink.JsonObjectProcessor;
import ixa.kaflib.KAFDocument;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Filters out documents that do not contain the desired relations and assigns priority rating
 */
public class FilterAnnotatedSentences extends RichFlatMapFunction<KAFDocument, Tuple3<String, Integer, String>> implements JsonObjectProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterAnnotatedSentences.class);
    private static final long serialVersionUID = 1L;

    public static final int FILTER_OUT = -1;
    public static final int NORMAL_PRIORITY = 0;
    public static final int THIRD_PRIORITY = 1;
    public static final int SECOND_PRIORITY = 2;
    public static final int HIGH_PRIORITY = 3;

    private final String dataFolder;

    private transient HashMap<String, Relation> relations;
    private transient HashMap<String, String> synsets;

    public FilterAnnotatedSentences(String dataFolder) {
        this.dataFolder = dataFolder;
    }

    @Override
    public void open(Configuration configuration) throws IOException {
        restoreData(dataFolder);
    }

    @Override
    public void flatMap(KAFDocument document, Collector<Tuple3<String, Integer, String>> out) throws Exception {
        int priority = FILTER_OUT;
        HashSet<String> objects = new HashSet<>();
        List<Relation> rels = new LinkedList<>();
        document.getTerms().forEach(entity -> {
            entity.getExternalRefs().forEach(ref -> {
                String refValue = ref.getReference();

                if (!refValue.matches("[0-9]{8}-[a-z]")) {
                    return;
                }

                if (!synsets.containsKey(refValue)) {
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

        StringBuilder extractedMatches = new StringBuilder();
        priority = NORMAL_PRIORITY;
        if (matches > 1) {
            priority = THIRD_PRIORITY;
        }

        for (Relation relation : rels) {
            Sets.SetView<String> filledObjRoles = Sets.intersection(relation.objects, objects);
            Sets.SetView<String> filledSubjRoles = Sets.intersection(relation.subjects, objects);

            if (Sets.difference(filledObjRoles, filledSubjRoles).size() > 0 && filledObjRoles.size() > 0 && filledSubjRoles.size() > 0) {
                if (extractedMatches.length() != 0) {
                    extractedMatches.append(",");
                }
                extractedMatches.append("<");
                assembleMatches(filledObjRoles, extractedMatches);
                extractedMatches.append(",");
                extractedMatches.append(synsets.get(relation.synset));
                extractedMatches.append(",");
                assembleMatches(filledSubjRoles, extractedMatches);
                extractedMatches.append(">");

                priority = HIGH_PRIORITY;
                break;
            } else if (filledObjRoles.size() + filledSubjRoles.size() > 0) {
                priority = SECOND_PRIORITY;
            }
        }

        getRuntimeContext().getIntCounter("PRIORITY_"+priority).add(1);
        out.collect(new Tuple3<>(document.getRawText(), priority, extractedMatches.toString()));
    }

    private void assembleMatches(Set<String> matches, StringBuilder builder) {
        if (matches.size() > 1) {
            builder.append("(");
        }

        List<String> convertedMatches = matches.stream().map(s -> synsets.get(s)).collect(Collectors.toList());
        builder.append(String.join("|", convertedMatches));

        if (matches.size() > 1) {
            builder.append(")");
        }
    }

    private void restoreData(String directory) throws IOException {
        relations = new HashMap<>();
        synsets = new HashMap<>();

        File relationsFile = new File(directory, "relations.tsv");
        File synsetsFile = new File(directory, "synsets.tsv");

        try (CSVParser relationsParser = new CSVParser(new FileReader(relationsFile), CSVFormat.TDF)) {
            relationsParser.forEach(record -> relations.put(record.get(1), Relation.processRecord(record)));
        }

        try (CSVParser synsetParser = new CSVParser(new FileReader(synsetsFile), CSVFormat.TDF)) {
            synsetParser.forEach(record -> synsets.put(record.get(0), record.get(1)));
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
