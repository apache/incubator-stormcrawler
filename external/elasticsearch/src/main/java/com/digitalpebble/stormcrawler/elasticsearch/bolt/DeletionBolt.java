package com.digitalpebble.stormcrawler.elasticsearch.bolt;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.Client;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.elasticsearch.ElasticSearchConnection;
import com.digitalpebble.stormcrawler.util.ConfUtils;

/**
 * Deletes documents to ElasticSearch. This should be connected to the
 * StatusUpdaterBolt via the 'deletion' stream and will remove the documents
 * with a status of ERROR one by one. Note that this component will also try to
 * delete documents even though they were never indexed and it currently won't
 * delete documents which were indexed under the canonical URL.
 */
public class DeletionBolt extends BaseRichBolt {

    static final org.slf4j.Logger LOG = LoggerFactory.getLogger(MethodHandles
            .lookup().lookupClass());

    private static final String ESBoltType = "indexer";

    private OutputCollector _collector;

    private String indexName;
    private String docType;

    private Client client;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void prepare(Map conf, TopologyContext context,
            OutputCollector collector) {
        _collector = collector;

        indexName = ConfUtils.getString(conf, IndexerBolt.ESIndexNameParamName,
                "fetcher");
        docType = ConfUtils.getString(conf, IndexerBolt.ESDocTypeParamName,
                "doc");
        client = ElasticSearchConnection.getClient(conf, ESBoltType);
    }

    @Override
    public void cleanup() {
        if (client != null)
            client.close();
    }

    @Override
    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url");
        // keep it simple for now and ignore cases where the canonical URL was
        // used
        String sha256hex = org.apache.commons.codec.digest.DigestUtils
                .sha256Hex(url);
        DeleteRequest dr = new DeleteRequest(indexName, docType, sha256hex);
        client.delete(dr).actionGet();
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        // none
    }

}
