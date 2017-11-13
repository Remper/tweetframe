package eu.fbk.fm.tweetframe.pipeline.text;

import ixa.kaflib.ExternalRef;
import ixa.kaflib.KAFDocument;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Optional;

public class FrameDataFromKAF extends RichFlatMapFunction<KAFDocument, String> {

    @Override
    public void flatMap(KAFDocument value, Collector<String> out) throws Exception {
        value.getPredicates().forEach(predicate -> {
            String predicateRef = Optional
                                    .ofNullable(predicate.getExternalRef("FrameNet"))
                                    .map(ExternalRef::getReference)
                                    .orElse(null);
            if (predicateRef == null) {
                return;
            }

            StringBuilder sb = new StringBuilder();
            predicate.getRoles().forEach(role -> {
                String roleRef = Optional.ofNullable(role.getExternalRef("FrameNet"))
                        .map(ExternalRef::getReference)
                        .filter(r -> r.lastIndexOf('@') > -1)
                        .map(r -> r.substring(r.lastIndexOf('@')+1))
                        .orElse(null);
                if (roleRef == null) {
                    return;
                }

                String roleValue = role.getStr();

                if (sb.length() > 0) {
                    sb.append(" and ");
                }
                sb.append("'");
                sb.append(roleValue);
                sb.append("' is the '");
                sb.append(roleRef);
                sb.append("'");
            });

            out.collect(String.format("In frame %s %s", predicateRef.toUpperCase(), sb.toString()));
        });
    }
}
