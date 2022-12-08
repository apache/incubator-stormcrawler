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
package com.digitalpebble.stormcrawler.opensearch;

import static org.opensearch.client.RestClientBuilder.DEFAULT_CONNECT_TIMEOUT_MILLIS;
import static org.opensearch.client.RestClientBuilder.DEFAULT_SOCKET_TIMEOUT_MILLIS;

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
import org.jetbrains.annotations.NotNull;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.Node;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class to instantiate an ES client and bulkprocessor based on the configuration. */
public final class OpensearchConnection {

    private static final Logger LOG = LoggerFactory.getLogger(OpensearchConnection.class);

    @NotNull private final RestHighLevelClient client;

    @NotNull private final BulkProcessor processor;

    private OpensearchConnection(@NotNull RestHighLevelClient c, @NotNull BulkProcessor p) {
        processor = p;
        client = c;
    }

    public RestHighLevelClient getClient() {
        return client;
    }

    public void addToProcessor(final IndexRequest request) {
        processor.add(request);
    }

    public static RestHighLevelClient getClient(Map<String, Object> stormConf, String boltType) {

        List<String> confighosts =
                ConfUtils.loadListFromConf(
                        Constants.PARAMPREFIX + boltType + ".addresses", stormConf);

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
        String user = ConfUtils.getString(stormConf, Constants.PARAMPREFIX + boltType + ".user");
        String password =
                ConfUtils.getString(stormConf, Constants.PARAMPREFIX + boltType + ".password");

        String proxyhost =
                ConfUtils.getString(stormConf, Constants.PARAMPREFIX + boltType + ".proxy.host");

        int proxyport =
                ConfUtils.getInt(stormConf, Constants.PARAMPREFIX + boltType + ".proxy.port", -1);

        String proxyscheme =
                ConfUtils.getString(
                        stormConf, Constants.PARAMPREFIX + boltType + ".proxy.scheme", "http");

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
                        Constants.PARAMPREFIX + boltType + ".connect.timeout",
                        DEFAULT_CONNECT_TIMEOUT_MILLIS);
        int socketTimeout =
                ConfUtils.getInt(
                        stormConf,
                        Constants.PARAMPREFIX + boltType + ".socket.timeout",
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
        // int maxRetryTimeout = ConfUtils.getInt(stormConf, Constants.PARAMPREFIX +
        // boltType +
        // ".max.retry.timeout",
        // DEFAULT_MAX_RETRY_TIMEOUT_MILLIS);
        // builder.setMaxRetryTimeoutMillis(maxRetryTimeout);

        // TODO configure headers etc...
        // Map<String, String> configSettings = (Map) stormConf
        // .get(Constants.PARAMPREFIX + boltType + ".settings");
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
                ConfUtils.getBoolean(
                        stormConf, Constants.PARAMPREFIX + boltType + ".compression", false);

        builder.setCompressionEnabled(compression);

        return new RestHighLevelClient(builder);
    }

    /**
     * Creates a connection with a default listener. The values for bolt type are
     * [indexer,status,metrics]
     */
    public static OpensearchConnection getConnection(
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

    public static OpensearchConnection getConnection(
            Map<String, Object> stormConf, String boltType, BulkProcessor.Listener listener) {

        final RestHighLevelClient client = getClient(stormConf, boltType);

        final String flushIntervalString =
                ConfUtils.getString(
                        stormConf, Constants.PARAMPREFIX + boltType + ".flushInterval", "5s");

        final TimeValue flushInterval =
                TimeValue.parseTimeValue(
                        flushIntervalString, TimeValue.timeValueSeconds(5), "flushInterval");

        final int bulkActions =
                ConfUtils.getInt(stormConf, Constants.PARAMPREFIX + boltType + ".bulkActions", 50);

        final int concurrentRequests =
                ConfUtils.getInt(
                        stormConf, Constants.PARAMPREFIX + boltType + ".concurrentRequests", 1);

        final BulkProcessor bulkProcessor =
                BulkProcessor.builder(
                                (request, bulkListener) ->
                                        client.bulkAsync(
                                                request, RequestOptions.DEFAULT, bulkListener),
                                listener)
                        .setFlushInterval(flushInterval)
                        .setBulkActions(bulkActions)
                        .setConcurrentRequests(concurrentRequests)
                        .build();

        return new OpensearchConnection(client, bulkProcessor);
    }

    private boolean isClosed = false;

    public void close() {

        if (isClosed) {
            LOG.warn("Tried to close an already closed connection!");
            return;
        }

        // Maybe some kind of identifier?
        LOG.debug("Start closing the opensearchConnection");

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
