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

package com.digitalpebble.storm.crawler.elasticsearch.metrics;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;

import com.digitalpebble.storm.crawler.elasticsearch.ElasticSearchConnection;
import com.digitalpebble.storm.crawler.util.ConfUtils;

public class MetricsConsumer implements IMetricsConsumer {

    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private static final String ESBoltType = "metrics";

    private static final String ESMetricsIndexNameParamName = "es."
            + ESBoltType + ".index.name";
    private static final String ESmetricsDocTypeParamName = "es." + ESBoltType
            + ".doc.type";

    private String indexName;
    private String docType;

    private ElasticSearchConnection connection;

    @Override
    public void prepare(Map stormConf, Object registrationArgument,
            TopologyContext context, IErrorReporter errorReporter) {
        indexName = ConfUtils.getString(stormConf, ESMetricsIndexNameParamName,
                "metrics");
        docType = ConfUtils.getString(stormConf, ESmetricsDocTypeParamName,
                "datapoint");

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
    public void handleDataPoints(TaskInfo taskInfo,
            Collection<DataPoint> dataPoints) {

        final Iterator<DataPoint> datapointsIterator = dataPoints.iterator();
        while (datapointsIterator.hasNext()) {
            final DataPoint dataPoint = datapointsIterator.next();

            String name = dataPoint.name;

            Date now = new Date();

            if (dataPoint.value instanceof Map) {
                Iterator<Entry> keyValiter = ((Map) dataPoint.value).entrySet()
                        .iterator();
                while (keyValiter.hasNext()) {
                    Entry entry = keyValiter.next();

                    String temp = String.valueOf(entry.getValue());
                    Double value = Double.parseDouble(temp);

                    try {
                        XContentBuilder builder = jsonBuilder().startObject();
                        builder.field("srcComponentId", taskInfo.srcComponentId);
                        builder.field("srcTaskId", taskInfo.srcTaskId);
                        builder.field("srcWorkerHost", taskInfo.srcWorkerHost);
                        builder.field("srcWorkerPort", taskInfo.srcWorkerPort);
                        builder.field("name", name);
                        builder.field("key", entry.getKey());
                        builder.field("value", value);
                        builder.field("timestamp", now);

                        builder.endObject();

                        IndexRequestBuilder request = connection.getClient()
                                .prepareIndex(indexName, docType)
                                .setSource(builder);

                        // set TTL?
                        // what to use for id?

                        connection.getProcessor().add(request.request());
                    } catch (Exception e) {
                        LOG.error("problem when building request for ES", e);
                    }
                }
            } else {
                LOG.info("Found data point value of class {}", dataPoint.value
                        .getClass().toString());
            }
        }

    }
}
