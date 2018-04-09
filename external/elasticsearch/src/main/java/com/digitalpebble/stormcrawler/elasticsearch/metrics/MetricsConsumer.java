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

package com.digitalpebble.stormcrawler.elasticsearch.metrics;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.elasticsearch.ElasticSearchConnection;
import com.digitalpebble.stormcrawler.util.ConfUtils;

public class MetricsConsumer implements IMetricsConsumer {

    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private static final String ESBoltType = "metrics";

    /** name of the index to use for the metrics (default : metrics) **/
    private static final String ESMetricsIndexNameParamName = "es."
            + ESBoltType + ".index.name";

    /**
     * name of the document type to use for the metrics (default : datapoint)
     **/
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
        final Date now = new Date();
        for (DataPoint dataPoint : dataPoints) {
            handleDataPoints(taskInfo, dataPoint.name, dataPoint.value, now);
        }
    }

    private void handleDataPoints(final TaskInfo taskInfo,
            final String nameprefix, final Object value, final Date now) {
        if (value instanceof Number) {
            indexDataPoint(taskInfo, now, nameprefix,
                    ((Number) value).doubleValue());
        } else if (value instanceof Map) {
            Iterator<Entry> keyValiter = ((Map) value).entrySet().iterator();
            while (keyValiter.hasNext()) {
                Entry entry = keyValiter.next();
                String newnameprefix = nameprefix + "." + entry.getKey();
                handleDataPoints(taskInfo, newnameprefix, entry.getValue(), now);
            }
        } else if (value instanceof Collection) {
            for (Object collectionObj : (Collection) value) {
                handleDataPoints(taskInfo, nameprefix, collectionObj, now);
            }
        } else {
            LOG.warn("Found data point value {} of {}", nameprefix, value
                    .getClass().toString());
        }
    }

    /**
     * Returns the name of the index that metrics will be written too.
     * 
     * @return elastic index name
     */
    protected String getIndexName() {
        return indexName;
    }

    private void indexDataPoint(TaskInfo taskInfo, Date timestamp, String name,
            double value) {
        try {
            XContentBuilder builder = jsonBuilder().startObject();
            builder.field("srcComponentId", taskInfo.srcComponentId);
            builder.field("srcTaskId", taskInfo.srcTaskId);
            builder.field("srcWorkerHost", taskInfo.srcWorkerHost);
            builder.field("srcWorkerPort", taskInfo.srcWorkerPort);
            builder.field("name", name);
            builder.field("value", value);
            builder.field("timestamp", timestamp);
            builder.endObject();

            IndexRequest indexRequest = new IndexRequest(getIndexName(),
                    docType).source(builder);
            connection.getProcessor().add(indexRequest);
        } catch (Exception e) {
            LOG.error("problem when building request for ES", e);
        }
    }
}
