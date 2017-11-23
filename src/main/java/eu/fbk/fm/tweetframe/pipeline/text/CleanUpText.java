package eu.fbk.fm.tweetframe.pipeline.text;

import ixa.kaflib.ExternalRef;
import ixa.kaflib.KAFDocument;
import ixa.kaflib.Term;
import org.apache.flink.api.common.functions.MapFunction;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Replace numbers with <nmb> token, pack the rest into list
 */
public class CleanUpText implements MapFunction<KAFDocument, List<CleanUpText.SimpleTerm>> {

    @Override
    public List<SimpleTerm> map(KAFDocument kafDocument) throws Exception {
        LinkedList<SimpleTerm> result = new LinkedList<>();
        SimpleTerm lastCD = null;

        for (Term term : kafDocument.getTerms()) {
            String posTag = term.getMorphofeat();
            if (posTag.equals("CD")) {
                lastCD = SimpleTerm.fromTerm(term);
                continue;
            }

            if (lastCD != null && lastCD.pos.equals("CD")
                    && (posTag.equals(":") || posTag.equals(",") || posTag.equals("."))) {
                lastCD = SimpleTerm.fromTerm(term);
                continue;
            }

            if (lastCD != null) {
                result.add(new SimpleTerm("<nmb>", "CD"));
                if (!lastCD.pos.equals("CD")) {
                    result.add(lastCD);
                }
            }

            result.add(SimpleTerm.fromTerm(term));
        }

        return result;
    }

    public static class SimpleTerm implements Serializable {
        public final String lemma;
        public final String pos;
        public final String synset;

        public SimpleTerm(String lemma, String pos) {
            this(lemma, pos, null);
        }

        public SimpleTerm(String lemma, String pos, String synset) {
            this.lemma = lemma;
            this.pos = pos;
            this.synset = synset;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(lemma);
            sb.append("@");
            sb.append(pos);
            if (synset != null) {
                sb.append("@");
                sb.append(synset);
            }
            return sb.toString();
        }

        public static SimpleTerm fromString(String input) {
            String[] parts = input.split("@");
            if (parts.length < 2) {
                return null;
            }

            String synset = null;
            if (parts.length > 2) {
                synset = parts[2];
            }
            return new SimpleTerm(parts[0], parts[1], synset);
        }

        public static SimpleTerm fromTerm(Term term) {
            String synset = null;
            for (ExternalRef ref : term.getExternalRefs()) {
                if (ref.getReference().matches("[0-9]{8}-[a-z]")) {
                    synset = ref.getReference();
                    break;
                }
            }
            return new SimpleTerm(term.getForm(), term.getMorphofeat(), synset);
        }
    }
}
