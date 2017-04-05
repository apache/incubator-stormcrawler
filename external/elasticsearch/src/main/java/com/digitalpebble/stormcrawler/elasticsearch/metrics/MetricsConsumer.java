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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.elasticsearch.action.index.IndexRequestBuilder;
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

    /**
     * List of whitelisted metrics. Only store metrics with names that are one
     * of these strings
     **/
    private static final String ESmetricsWhitelistParamName = "es."
            + ESBoltType + ".whitelist";

    /**
     * List of blacklisted metrics. Never store metrics with names that are one
     * of these strings
     **/
    private static final String ESmetricsBlacklistParamName = "es."
            + ESBoltType + ".blacklist";

    private String indexName;
    private String docType;

    private ElasticSearchConnection connection;
    private String[] whitelist = new String[0];
    private String[] blacklist = new String[0];

    @Override
    public void prepare(Map stormConf, Object registrationArgument,
            TopologyContext context, IErrorReporter errorReporter) {

        indexName = ConfUtils.getString(stormConf, ESMetricsIndexNameParamName,
                "metrics");
        docType = ConfUtils.getString(stormConf, ESmetricsDocTypeParamName,
                "datapoint");

        setWhitelist(ConfUtils.loadListFromConf(ESmetricsWhitelistParamName,
                stormConf));
        setBlacklist(ConfUtils.loadListFromConf(ESmetricsBlacklistParamName,
                stormConf));

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
                    if (!(entry.getValue() instanceof Number)) {
                        LOG.error("Found data point value {} of class {}",
                                name, dataPoint.value.getClass().toString());
                        continue;
                    }
                    Double value = ((Number) entry.getValue()).doubleValue();
                    indexDataPoint(taskInfo, now, name + "." + entry.getKey(),
                            value);
                }
            } else if (dataPoint.value instanceof Number) {
                indexDataPoint(taskInfo, now, name,
                        ((Number) dataPoint.value).doubleValue());
            } else if (dataPoint.value instanceof Collection) {
                for (Object collectionObj : (Collection) dataPoint.value) {
                    if (!(collectionObj instanceof Number)) {
                        LOG.error("Found data point value {} of class {}",
                                name, dataPoint.value.getClass().toString());
                        continue;
                    }
                    Double value = ((Number) collectionObj).doubleValue();
                    indexDataPoint(taskInfo, now, name, value);
                }
            } else {
                LOG.error("Found data point value {} of class {}", name,
                        dataPoint.value.getClass().toString());
            }
        }
    }

    void setWhitelist(List<String> whitelist) {
        this.whitelist = whitelist.toArray(new String[whitelist.size()]);
    }

    void setBlacklist(List<String> blacklist) {
        this.blacklist = blacklist.toArray(new String[blacklist.size()]);
    }

    boolean shouldSkip(String name) {
        if (StringUtils.startsWithAny(name, blacklist)) {
            return true;
        }
        if (whitelist.length > 0) {
            return !StringUtils.startsWithAny(name, whitelist);
        }
        return false;
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
        if (shouldSkip(name)) {
            return;
        }
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

            IndexRequestBuilder request = connection.getClient()
                    .prepareIndex(getIndexName(), docType).setSource(builder);

            connection.getProcessor().add(request.request());
        } catch (Exception e) {
            LOG.error("problem when building request for ES", e);
        }
    }
}
