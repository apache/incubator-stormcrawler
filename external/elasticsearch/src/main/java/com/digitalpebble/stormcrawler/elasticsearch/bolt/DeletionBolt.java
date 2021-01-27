/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.digitalpebble.stormcrawler.elasticsearch.bolt;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
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

    private RestHighLevelClient client;

    public DeletionBolt() {
    }

    /** Sets the index name instead of taking it from the configuration. **/
    public DeletionBolt(String indexName) {
        this.indexName = indexName;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void prepare(Map conf, TopologyContext context,
            OutputCollector collector) {
        _collector = collector;
        if (indexName == null) {
            indexName = ConfUtils.getString(conf,
                    IndexerBolt.ESIndexNameParamName, "content");
        }
        client = ElasticSearchConnection.getClient(conf, ESBoltType);
    }

    @Override
    public void cleanup() {
        if (client != null)
            try {
                client.close();
            } catch (IOException e) {
            }
    }

    @Override
    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url");
        Metadata metadata = (Metadata) tuple.getValueByField("metadata");

        // keep it simple for now and ignore cases where the canonical URL was
        // used
        String sha256hex = org.apache.commons.codec.digest.DigestUtils
                .sha256Hex(url);
        DeleteRequest dr = new DeleteRequest(getIndexName(metadata), sha256hex);
        try {
            client.delete(dr, RequestOptions.DEFAULT);
        } catch (IOException e) {
            _collector.fail(tuple);
            LOG.error("Exception caught while deleting", e);
            return;
        }
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        // none
    }

    /**
     * Must be overridden for implementing custom index names based on some
     * metadata information By Default, indexName coming from config is used
     */
    protected String getIndexName(Metadata m) {
        return indexName;
    }

}
