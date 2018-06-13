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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;

import com.digitalpebble.stormcrawler.util.ConfUtils;

/**
 * Utility class to instantiate an ES client and bulkprocessor based on the
 * configuration.
 **/
public class ElasticSearchConnection {

    private RestHighLevelClient client;

    private BulkProcessor processor;

    private ElasticSearchConnection(RestHighLevelClient c, BulkProcessor p) {
        processor = p;
        client = c;
    }

    public RestHighLevelClient getClient() {
        return client;
    }

    public BulkProcessor getProcessor() {
        return processor;
    }

    public static RestHighLevelClient getClient(Map stormConf, String boltType) {

        List<String> confighosts = ConfUtils.loadListFromConf("es." + boltType
                + ".addresses", stormConf);

        List<HttpHost> hosts = new ArrayList<>();

        for (String host : confighosts) {
            // no port specified? use default one
            int port = 9200;
            String scheme = "http";
            // no scheme specified? use http
            if (!host.startsWith(scheme)) {
                host = "http://" + host;
            }
            URI uri = URI.create(host);
            if (uri.getHost() == null) {
                throw new RuntimeException("host undefined " + host);
            }
            if (uri.getPort() != -1) {
                port = uri.getPort();
            }
            if (uri.getScheme() != null) {
                scheme = uri.getScheme();
            }
            hosts.add(new HttpHost(uri.getHost(), port, scheme));
        }

        RestClientBuilder builder = RestClient.builder(hosts
                .toArray(new HttpHost[hosts.size()]));

        // authentication via user / password
        String user = ConfUtils
                .getString(stormConf, "es." + boltType + ".user");
        String password = ConfUtils.getString(stormConf, "es." + boltType
                + ".password");

        if (StringUtils.isNotBlank(user) && StringUtils.isNotBlank(password)) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(user, password));
            builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(
                        HttpAsyncClientBuilder httpClientBuilder) {
                    return httpClientBuilder
                            .setDefaultCredentialsProvider(credentialsProvider);
                }
            });
        }

        // TODO configure headers etc...
        // Map<String, String> configSettings = (Map) stormConf
        // .get("es." + boltType + ".settings");
        // if (configSettings != null) {
        // configSettings.forEach((k, v) -> settings.put(k, v));
        // }

        return new RestHighLevelClient(builder);
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

        String flushIntervalString = ConfUtils.getString(stormConf,
                "es." + boltType + ".flushInterval", "5s");

        TimeValue flushInterval = TimeValue.parseTimeValue(flushIntervalString,
                TimeValue.timeValueSeconds(5), "flushInterval");

        int bulkActions = ConfUtils.getInt(stormConf,
                "es." + boltType + ".bulkActions", 50);

        int concurrentRequests = ConfUtils.getInt(stormConf,
                "es." + boltType + ".concurrentRequests", 1);

        RestHighLevelClient client = getClient(stormConf, boltType);

        BulkProcessor bulkProcessor = BulkProcessor
                .builder(client::bulkAsync, listener)
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
            try {
                client.close();
            } catch (IOException e) {
                // ignore silently
            }
        }
    }
}
