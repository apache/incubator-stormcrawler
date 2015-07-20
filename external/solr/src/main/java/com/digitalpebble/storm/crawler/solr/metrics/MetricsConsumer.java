/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.digitalpebble.storm.crawler.solr.metrics;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;

import com.digitalpebble.storm.crawler.solr.SolrConnection;
import com.digitalpebble.storm.crawler.util.ConfUtils;

public class MetricsConsumer implements IMetricsConsumer {

    private final Logger LOG = LoggerFactory.getLogger(MetricsConsumer.class);

    private static final String BOLT_TYPE = "metrics";

    private static final String SolrIndexCollection = "solr.metrics.collection";
    private static final String SolrBatchSizeParam = "solr.metrics.commit.size";
    private static final String SolrTTLParamName = "solr.metrics.ttl";
    private static final String SolrTTLFieldParamName = "solr.metrics.ttl.field";

    private String collection;
    private String ttlField;
    private String ttl;
    private int batchSize;
    private int counter = 0;

    private SolrConnection connection;

    @Override
    public void prepare(Map stormConf, Object registrationArgument,
            TopologyContext topologyContext, IErrorReporter errorReporter) {

        collection = ConfUtils.getString(stormConf, SolrIndexCollection,
                "metrics");
        ttlField = ConfUtils.getString(stormConf, SolrTTLFieldParamName,
                "__ttl__");
        ttl = ConfUtils.getString(stormConf, SolrTTLParamName, null);
        batchSize = ConfUtils.getInt(stormConf, SolrBatchSizeParam, 250);

        try {
            connection = SolrConnection.getConnection(stormConf, BOLT_TYPE);
        } catch (Exception e) {
            LOG.error("Can't connect to Solr: {}", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void handleDataPoints(TaskInfo taskInfo,
            Collection<DataPoint> dataPoints) {

        final Iterator<DataPoint> datapointsIterator = dataPoints.iterator();

        while (datapointsIterator.hasNext()) {
            final DataPoint dataPoint = datapointsIterator.next();

            String name = dataPoint.name;

            Date now = new Date();

            if (dataPoint.value instanceof Map) {
                Iterator<Map.Entry> keyValiter = ((Map) dataPoint.value)
                        .entrySet().iterator();
                while (keyValiter.hasNext()) {
                    Map.Entry entry = keyValiter.next();
                    if (!(entry.getValue() instanceof Number)) {
                        LOG.error("Found data point value of class {}", entry
                                .getValue().getClass().toString());
                        continue;
                    }
                    Double value = ((Number) entry.getValue()).doubleValue();
                    indexDataPoint(taskInfo, now, name + "." + entry.getKey(),
                            value);
                    counter++;
                }
            } else if (dataPoint.value instanceof Number) {
                indexDataPoint(taskInfo, now, name,
                        ((Number) dataPoint.value).doubleValue());
                counter++;
            } else {
                LOG.error("Found data point value of class {}", dataPoint.value
                        .getClass().toString());
            }

            try {
                if (counter % batchSize == 0 || (!datapointsIterator.hasNext())) {
                    connection.getClient().commit();
                    counter = 0;
                }
            } catch (Exception e) {
                LOG.error("Send metric to Solr failed due to", e);
            }
        }
    }

    private void indexDataPoint(TaskInfo taskInfo, Date timestamp, String name,
            double value) {
        try {
            SolrInputDocument doc = new SolrInputDocument();

            doc.addField("srcComponentId", taskInfo.srcComponentId);
            doc.addField("srcTaskId", taskInfo.srcTaskId);
            doc.addField("srcWorkerHost", taskInfo.srcWorkerHost);
            doc.addField("srcWorkerPort", taskInfo.srcWorkerPort);
            doc.addField("name", name);
            doc.addField("value", value);
            doc.addField("timestamp", timestamp);

            if (this.ttl != null) {
                doc.addField(ttlField, ttl);
            }

            connection.getClient().add(doc);
        } catch (Exception e) {
            LOG.error("Problem building a document to Solr", e);
        }
    }

    @Override
    public void cleanup() {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                LOG.error("Can't close connection to Solr", e);
            }
        }
    }
}
