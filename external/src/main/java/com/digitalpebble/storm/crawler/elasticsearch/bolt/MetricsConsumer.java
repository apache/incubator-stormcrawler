package com.digitalpebble.storm.crawler.elasticsearch.bolt;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;

import com.digitalpebble.storm.crawler.util.ConfUtils;

public class MetricsConsumer implements IMetricsConsumer {

    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private static final String ESMetricsIndexNameParamName = "es.metrics.index.name";
    private static final String ESmetricsDocTypeParamName = "es.metrics.doc.type";
    private static final String ESmetricsHostParamName = "es.metrics.hostname";

    private String indexName;
    private String docType;
    private String host;

    private Client client;

    private BulkProcessor bulkProcessor;

    @Override
    public void prepare(Map stormConf, Object registrationArgument,
            TopologyContext context, IErrorReporter errorReporter) {
        indexName = ConfUtils.getString(stormConf, ESMetricsIndexNameParamName,
                "metrics");
        docType = ConfUtils.getString(stormConf, ESmetricsDocTypeParamName,
                "datapoint");
        host = ConfUtils.getString(stormConf, ESmetricsHostParamName,
                "localhost");

        // connection to ES
        try {
            if (host.equalsIgnoreCase("localhost")) {
                Node node = org.elasticsearch.node.NodeBuilder.nodeBuilder()
                        .clusterName("elasticsearch").client(true).node();
                client = node.client();
            } else {
                Settings settings = ImmutableSettings.settingsBuilder()
                        .put("cluster.name", "elasticsearch").build();
                client = new TransportClient(settings)
                        .addTransportAddress(new InetSocketTransportAddress(
                                host, 9300));
            }

        } catch (Exception e1) {
            LOG.error("Can't connect to ElasticSearch", e1);
            throw new RuntimeException(e1);
        }
        bulkProcessor = BulkProcessor
                .builder(client, new BulkProcessor.Listener() {

                    @Override
                    public void afterBulk(long arg0, BulkRequest arg1,
                            BulkResponse arg2) {
                        arg2.toString();
                    }

                    @Override
                    public void afterBulk(long arg0, BulkRequest arg1,
                            Throwable arg2) {
                    }

                    @Override
                    public void beforeBulk(long arg0, BulkRequest arg1) {
                    }

                }).setFlushInterval(TimeValue.timeValueSeconds(5))
                .setBulkActions(50).setConcurrentRequests(1).build();
    }

    @Override
    public void cleanup() {
        client.close();
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

                        IndexRequestBuilder request = client.prepareIndex(
                                indexName, docType).setSource(builder);

                        // set TTL?
                        // what to use for id?

                        bulkProcessor.add(request.request());
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
