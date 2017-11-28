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

package com.digitalpebble.stormcrawler.elasticsearch;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.digitalpebble.stormcrawler.util.ConfUtils;

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

        // https://github.com/DigitalPebble/storm-crawler/issues/493
        System.setProperty("es.set.netty.runtime.available.processors", "false");

        Builder settings = Settings.builder();

        Map configSettings = (Map) stormConf
                .get("es." + boltType + ".settings");
        if (configSettings != null) {
            settings.put(configSettings);
        }

        List<String> pluginList = ConfUtils.loadListFromConf("es." + boltType
                + ".plugins", stormConf);
        List<Class<? extends Plugin>> pluginClasses = new LinkedList<>();
        for (String plugin : pluginList) {
            try {
                Class pluginClass = Class.forName(plugin);
                pluginClasses.add(pluginClass);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        TransportClient tc = new PreBuiltTransportClient(settings.build(),
                pluginClasses);

        List<String> hosts = ConfUtils.loadListFromConf("es." + boltType
                + ".addresses", stormConf);

        for (String host : hosts) {
            String[] hostPort = host.split(":");
            // no port specified? use default one
            int port = 9300;
            if (hostPort.length == 2) {
                port = Integer.parseInt(hostPort[1].trim());
            }
            try {
                TransportAddress ista = new TransportAddress(
                        InetAddress.getByName(hostPort[0].trim()), port);
                tc.addTransportAddress(ista);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }

        return tc;
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
                TimeValue.timeValueSeconds(5), "flushInterval");

        int bulkActions = ConfUtils.getInt(stormConf, "es." + boltType
                + ".bulkActions", 50);

        int concurrentRequests = ConfUtils.getInt(stormConf, "es." + boltType
                + ".concurrentRequests", 1);

        Client client = getClient(stormConf, boltType);

        BulkProcessor bulkProcessor = BulkProcessor.builder(client, listener)
                .setFlushInterval(flushInterval).setBulkActions(bulkActions)
                .setConcurrentRequests(concurrentRequests).build();

        return new ElasticSearchConnection(client, bulkProcessor);
    }

    public void close() {
        // First, close the BulkProcessor ensuring pending actions are flushed
        if (processor != null) {
            try {
                boolean success = processor.awaitClose(60, TimeUnit.SECONDS);
                if (!success) {
                    throw new RuntimeException(
                            "Failed to flush pending actions when closing BulkProcessor");
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // Now close the actual client
        if (client != null) {
            client.close();
        }
    }
}
