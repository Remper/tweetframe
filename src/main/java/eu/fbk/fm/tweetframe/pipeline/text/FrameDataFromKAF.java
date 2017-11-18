package eu.fbk.fm.tweetframe.pipeline.text;

import ixa.kaflib.ExternalRef;
import ixa.kaflib.KAFDocument;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Optional;

public class FrameDataFromKAF extends RichFlatMapFunction<Tuple2<KAFDocument,Integer>, String> {

    @Override
    public void flatMap(Tuple2<KAFDocument,Integer> value, Collector<String> out) throws Exception {
        value.f0.getPredicates().forEach(predicate -> {
            String predicateRef = Optional
                                    .ofNullable(predicate.getExternalRef("FrameNet"))
                                    .map(ExternalRef::getReference)
                                    .orElse(null);
            if (predicateRef == null) {
                return;
            }

            StringBuilder sb = new StringBuilder();
            predicate.getRoles().forEach(role -> {
                String roleRef = Optional
                                    .ofNullable(role.getExternalRef("FrameNet"))
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
                sb.append(roleValue.replaceAll("[^0-9a-zA-Z\\s]+", "").replaceAll("\\s+", " "));
                sb.append("' is the '");
                sb.append(roleRef);
                sb.append("'");
            });

            getRuntimeContext().getIntCounter("COLLECTED_FRAMES").add(1);
            out.collect(String.format("In frame (%d) %s %s", value.f1, predicateRef.toUpperCase(), sb.toString()));
        });
    }
}
