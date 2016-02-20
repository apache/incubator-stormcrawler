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

package com.digitalpebble.storm.crawler.elasticsearch;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;

import com.digitalpebble.storm.crawler.util.ConfUtils;

/**
 * Utility class to instantiate an ES client and bulkprocessor based on the
 * configuration.
 **/
public class ElasticSearchConnection {

    private Client client;

    private BulkProcessor processor;

    private ElasticSearchConnection(Client c, BulkProcessor p) {
        processor = p;
        client = c;
    }

    public Client getClient() {
        return client;
    }

    public BulkProcessor getProcessor() {
        return processor;
    }

    public static Client getClient(Map stormConf, String boltType) {
        String host = ConfUtils.getString(stormConf, "es." + boltType
                + ".hostname");

        String clustername = ConfUtils.getString(stormConf, "es." + boltType
                + ".cluster.name", "elasticsearch");

        // Use Node client if no host is specified
        // ES will try to find the cluster automatically
        // and join it
        if (StringUtils.isBlank(host)) {
            Node node = org.elasticsearch.node.NodeBuilder
                    .nodeBuilder()
                    .settings(
                            ImmutableSettings.settingsBuilder().put(
                                    "http.enabled", false))
                    .clusterName(clustername).client(true).node();
            return node.client();
        }

        // if a transport address has been specified
        // use the transport client - even if it is localhost
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", clustername).build();
        return new TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(host, 9300));
    }

    /**
     * Creates a connection with a default listener. The values for bolt type
     * are [indexer,status,metrics]
     **/
    public static ElasticSearchConnection getConnection(Map stormConf,
            String boltType) {
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void afterBulk(long arg0, BulkRequest arg1, BulkResponse arg2) {
            }

            @Override
            public void afterBulk(long arg0, BulkRequest arg1, Throwable arg2) {
            }

            @Override
            public void beforeBulk(long arg0, BulkRequest arg1) {
            }
        };
        return getConnection(stormConf, boltType, listener);
    }

    public static ElasticSearchConnection getConnection(Map stormConf,
            String boltType, BulkProcessor.Listener listener) {

        String flushIntervalString = ConfUtils.getString(stormConf, "es."
                + boltType + ".flushInterval", "5s");

        TimeValue flushInterval = TimeValue.parseTimeValue(flushIntervalString,
                TimeValue.timeValueSeconds(5));

        int bulkActions = ConfUtils.getInt(stormConf, "es." + boltType
                + ".bulkActions", 50);

        Client client = getClient(stormConf, boltType);

        BulkProcessor bulkProcessor = BulkProcessor.builder(client, listener)
                .setFlushInterval(flushInterval).setBulkActions(bulkActions)
                .setConcurrentRequests(1).build();

        return new ElasticSearchConnection(client, bulkProcessor);
    }

    public void close() {
        if (client != null)
            client.close();
        if (processor != null)
            processor.close();
    }
}
