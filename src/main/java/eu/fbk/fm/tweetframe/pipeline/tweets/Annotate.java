package eu.fbk.fm.tweetframe.pipeline.tweets;

import eu.fbk.fm.tweetframe.utils.flink.JsonObjectProcessor;
import ixa.kaflib.KAFDocument;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Annotates text using the Pikes pipeline
 */
public class Annotate extends RichFlatMapFunction<String, KAFDocument> implements JsonObjectProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(Annotate.class);
    private static final long serialVersionUID = 1L;

    private final String requestURI;

    private transient CloseableHttpClient httpclient;

    public Annotate(String requestURI) {
        this.requestURI = requestURI;
    }

    @Override
    public void open(Configuration configuration) throws IOException {
        httpclient = HttpClients.createDefault();
    }

    @Override
    public void flatMap(String value, Collector<KAFDocument> out) throws Exception {
        HttpPost request = new HttpPost(requestURI);
        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("text", value));
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

        out.collect(document);
    }
}
