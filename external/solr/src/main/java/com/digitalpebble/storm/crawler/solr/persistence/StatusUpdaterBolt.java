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

package com.digitalpebble.storm.crawler.solr.persistence;

import java.util.Date;
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.persistence.AbstractStatusUpdaterBolt;
import com.digitalpebble.storm.crawler.persistence.Status;
import com.digitalpebble.storm.crawler.solr.SolrConnection;
import com.digitalpebble.storm.crawler.util.ConfUtils;

public class StatusUpdaterBolt extends AbstractStatusUpdaterBolt {

    private static final Logger LOG = LoggerFactory
            .getLogger(StatusUpdaterBolt.class);

    private static final String BOLT_TYPE = "status";

    private static final String SolrIndexCollection = "solr.status.collection";
    private static final String SolrBatchSizeParam = "solr.status.commit.size";

    private String collection;
    private int batchSize;
    private int counter = 0;

    private SolrConnection connection;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {

        super.prepare(stormConf, context, collector);

        collection = ConfUtils.getString(stormConf, SolrIndexCollection,
                "status");
        batchSize = ConfUtils.getInt(stormConf, SolrBatchSizeParam, 250);

        try {
            connection = SolrConnection.getConnection(stormConf, BOLT_TYPE);
        } catch (Exception e) {
            LOG.error("Can't connect to Solr: {}", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void store(String url, Status status, Metadata metadata,
            Date nextFetch) throws Exception {

        SolrInputDocument doc = new SolrInputDocument();

        doc.setField("url", url);
        doc.setField("status", status);

        doc.setField("metadata", metadata.toString());
        doc.setField("nextFetchDate", nextFetch);

        connection.getClient().add(doc);
        counter++;

        if (counter % batchSize == 0) {
            try {
                connection.getClient().commit();
            } catch (Exception e) {
                LOG.error("Updating status for {} failed due to: {}", url, e);
            }

            counter = 0;
        }
    }

    @Override
    public void cleanup() {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                LOG.error("Can't close connection to Solr: {}", e);
            }
        }
    }
}
