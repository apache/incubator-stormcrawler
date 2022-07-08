/**
 * Licensed to DigitalPebble Ltd under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.digitalpebble.stormcrawler.elasticsearch;

import static org.elasticsearch.client.RestClientBuilder.DEFAULT_CONNECT_TIMEOUT_MILLIS;
import static org.elasticsearch.client.RestClientBuilder.DEFAULT_SOCKET_TIMEOUT_MILLIS;

import com.digitalpebble.stormcrawler.util.ConfUtils;
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
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestHighLevelClientBuilder;
import org.elasticsearch.client.sniff.Sniffer;
import org.elasticsearch.core.TimeValue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class to instantiate an ES client and bulkprocessor based on the configuration. */
public final class ElasticSearchConnection {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchConnection.class);

    @NotNull private final RestHighLevelClient client;

    @NotNull private final BulkProcessor processor;

    @Nullable private final Sniffer sniffer;

    private ElasticSearchConnection(@NotNull RestHighLevelClient c, @NotNull BulkProcessor p) {
        this(c, p, null);
    }

    private ElasticSearchConnection(
            @NotNull RestHighLevelClient c, @NotNull BulkProcessor p, @Nullable Sniffer s) {
        processor = p;
        client = c;
        sniffer = s;
    }

    public RestHighLevelClient getClient() {
        return client;
    }

    public void addToProcessor(final IndexRequest request) {
        processor.add(request);
    }

    public static RestHighLevelClient getClient(Map<String, Object> stormConf, String boltType) {

        List<String> confighosts =
                ConfUtils.loadListFromConf("es." + boltType + ".addresses", stormConf);

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

        RestClientBuilder builder = RestClient.builder(hosts.toArray(new HttpHost[0]));

        // authentication via user / password
        String user = ConfUtils.getString(stormConf, "es." + boltType + ".user");
        String password = ConfUtils.getString(stormConf, "es." + boltType + ".password");

        String proxyhost = ConfUtils.getString(stormConf, "es." + boltType + ".proxy.host");

        int proxyport = ConfUtils.getInt(stormConf, "es." + boltType + ".proxy.port", -1);

        String proxyscheme =
                ConfUtils.getString(stormConf, "es." + boltType + ".proxy.scheme", "http");

        boolean needsUser = StringUtils.isNotBlank(user) && StringUtils.isNotBlank(password);
        boolean needsProxy = StringUtils.isNotBlank(proxyhost) && proxyport != -1;

        if (needsUser || needsProxy) {
            builder.setHttpClientConfigCallback(
                    httpClientBuilder -> {
                        if (needsUser) {
                            final CredentialsProvider credentialsProvider =
                                    new BasicCredentialsProvider();
                            credentialsProvider.setCredentials(
                                    AuthScope.ANY, new UsernamePasswordCredentials(user, password));
                            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }
                        if (needsProxy) {
                            httpClientBuilder.setProxy(
                                    new HttpHost(proxyhost, proxyport, proxyscheme));
                        }
                        return httpClientBuilder;
                    });
        }

        int connectTimeout =
                ConfUtils.getInt(
                        stormConf,
                        "es." + boltType + ".connect.timeout",
                        DEFAULT_CONNECT_TIMEOUT_MILLIS);
        int socketTimeout =
                ConfUtils.getInt(
                        stormConf,
                        "es." + boltType + ".socket.timeout",
                        DEFAULT_SOCKET_TIMEOUT_MILLIS);
        // timeout until connection is established
        builder.setRequestConfigCallback(
                requestConfigBuilder ->
                        requestConfigBuilder
                                .setConnectTimeout(connectTimeout)
                                .setSocketTimeout(socketTimeout) // Timeout when waiting
                // for data
                );

        // TODO check if this has gone somewhere else in ES 7
        // int maxRetryTimeout = ConfUtils.getInt(stormConf, "es." + boltType +
        // ".max.retry.timeout",
        // DEFAULT_MAX_RETRY_TIMEOUT_MILLIS);
        // builder.setMaxRetryTimeoutMillis(maxRetryTimeout);

        // TODO configure headers etc...
        // Map<String, String> configSettings = (Map) stormConf
        // .get("es." + boltType + ".settings");
        // if (configSettings != null) {
        // configSettings.forEach((k, v) -> settings.put(k, v));
        // }

        // use node selector only to log nodes listed in the config
        // and/or discovered through sniffing
        builder.setNodeSelector(
                nodes -> {
                    for (Node node : nodes) {
                        LOG.debug(
                                "Connected to ES node {} [{}] for {}",
                                node.getName(),
                                node.getHost(),
                                boltType);
                    }
                });

        final boolean compression =
                ConfUtils.getBoolean(stormConf, "es." + boltType + ".compression", false);

        builder.setCompressionEnabled(compression);

        final boolean compatibilityMode =
                ConfUtils.getBoolean(stormConf, "es." + boltType + ".compatibility.mode", false);

        return new RestHighLevelClientBuilder(builder.build())
                .setApiCompatibilityMode(compatibilityMode)
                .build();
    }

    /**
     * Creates a connection with a default listener. The values for bolt type are
     * [indexer,status,metrics]
     */
    public static ElasticSearchConnection getConnection(
            Map<String, Object> stormConf, String boltType) {
        BulkProcessor.Listener listener =
                new BulkProcessor.Listener() {
                    @Override
                    public void afterBulk(long arg0, BulkRequest arg1, BulkResponse arg2) {}

                    @Override
                    public void afterBulk(long arg0, BulkRequest arg1, Throwable arg2) {}

                    @Override
                    public void beforeBulk(long arg0, BulkRequest arg1) {}
                };
        return getConnection(stormConf, boltType, listener);
    }

    public static ElasticSearchConnection getConnection(
            Map<String, Object> stormConf, String boltType, BulkProcessor.Listener listener) {

        String flushIntervalString =
                ConfUtils.getString(stormConf, "es." + boltType + ".flushInterval", "5s");

        TimeValue flushInterval =
                TimeValue.parseTimeValue(
                        flushIntervalString, TimeValue.timeValueSeconds(5), "flushInterval");

        int bulkActions = ConfUtils.getInt(stormConf, "es." + boltType + ".bulkActions", 50);

        int concurrentRequests =
                ConfUtils.getInt(stormConf, "es." + boltType + ".concurrentRequests", 1);

        RestHighLevelClient client = getClient(stormConf, boltType);

        boolean sniff = ConfUtils.getBoolean(stormConf, "es." + boltType + ".sniff", true);
        Sniffer sniffer = null;
        if (sniff) {
            sniffer = Sniffer.builder(client.getLowLevelClient()).build();
        }

        BulkProcessor bulkProcessor =
                BulkProcessor.builder(
                                (request, bulkListener) ->
                                        client.bulkAsync(
                                                request, RequestOptions.DEFAULT, bulkListener),
                                listener,
                                boltType + "-bulk-processor")
                        .setFlushInterval(flushInterval)
                        .setBulkActions(bulkActions)
                        .setConcurrentRequests(concurrentRequests)
                        .build();

        return new ElasticSearchConnection(client, bulkProcessor, sniffer);
    }

    private boolean isClosed = false;

    public void close() {

        if (isClosed) {
            LOG.warn("Tried to close an already closed connection!");
            return;
        }

        // Maybe some kind of identifier?
        LOG.debug("Start closing the ElasticSearchConnection");

        // First, close the BulkProcessor ensuring pending actions are flushed
        try {
            boolean success = processor.awaitClose(60, TimeUnit.SECONDS);
            if (!success) {
                throw new RuntimeException(
                        "Failed to flush pending actions when closing BulkProcessor");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (sniffer != null) {
            sniffer.close();
        }

        // Now close the actual client
        try {
            client.close();
        } catch (IOException e) {
            // ignore silently
            LOG.trace("Client threw IO exception.");
        }

        isClosed = true;
    }
}
