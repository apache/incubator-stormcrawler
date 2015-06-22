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

package com.digitalpebble.storm.crawler.elasticsearch.persistence;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.elasticsearch.ElasticSearchConnection;
import com.digitalpebble.storm.crawler.persistence.AbstractStatusUpdaterBolt;
import com.digitalpebble.storm.crawler.persistence.Status;
import com.digitalpebble.storm.crawler.util.ConfUtils;

/**
 * Simple bolt which stores the status of URLs into ElasticSearch. Takes the
 * tuples coming from the 'status' stream. To be used in combination with a
 * Spout to read from the index.
 **/
@SuppressWarnings("serial")
public class StatusUpdaterBolt extends AbstractStatusUpdaterBolt {

    private static final Logger LOG = LoggerFactory
            .getLogger(StatusUpdaterBolt.class);

    private static final String ESBoltType = "status";

    private static final String ESStatusIndexNameParamName = "es.status.index.name";
    private static final String ESStatusDocTypeParamName = "es.status.doc.type";
    private static final String ESStatusRoutingParamName = "es.status.metadata.routing";

    private String indexName;
    private String docType;
    /** route to shard based on the value of a metadata **/
    private String metadataRouting;

    private ElasticSearchConnection connection;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {

        super.prepare(stormConf, context, collector);

        indexName = ConfUtils.getString(stormConf,
                StatusUpdaterBolt.ESStatusIndexNameParamName, "status");
        docType = ConfUtils.getString(stormConf,
                StatusUpdaterBolt.ESStatusDocTypeParamName, "status");
        metadataRouting = ConfUtils.getString(stormConf,
                StatusUpdaterBolt.ESStatusRoutingParamName);

        try {
            connection = ElasticSearchConnection.getConnection(stormConf,
                    ESBoltType);
        } catch (Exception e1) {
            LOG.error("Can't connect to ElasticSearch", e1);
            throw new RuntimeException(e1);
        }
    }

    @Override
    public void cleanup() {
        if (connection != null)
            connection.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void store(String url, Status status, Metadata metadata,
            Date nextFetch) throws Exception {

        XContentBuilder builder = jsonBuilder().startObject();
        builder.field("url", url);
        // TODO update the status e.g more than N fetcherrors => error
        builder.field("status", status);

        // check that we don't overwrite an existing entry
        // When create is used, the index operation will fail if a document
        // by that id already exists in the index.
        boolean create = status.equals(Status.DISCOVERED);

        builder.field("metadata", metadata);
        builder.field("nextFetchDate", nextFetch);

        builder.endObject();

        IndexRequestBuilder request = connection.getClient()
                .prepareIndex(indexName, docType).setSource(builder)
                .setCreate(create).setId(url);

        if (StringUtils.isNotBlank(metadataRouting)) {
            String valueForRouting = metadata.getFirstValue(metadataRouting);
            if (StringUtils.isNotBlank(valueForRouting)) {
                request.setRouting(valueForRouting);
            }
        }

        connection.getProcessor().add(request.request());
    }

}
