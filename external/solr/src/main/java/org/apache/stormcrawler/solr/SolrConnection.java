/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.stormcrawler.solr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.apache.solr.client.solrj.impl.LBSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.stormcrawler.util.ConfUtils;

public class SolrConnection {

    private SolrClient client;
    private SolrClient updateClient;

    private static boolean cloud;
    private static String collection;

    private SolrConnection(SolrClient client, SolrClient updateClient) {
        this.client = client;
        this.updateClient = updateClient;
    }

    public SolrClient getClient() {
        return client;
    }

    public SolrClient getUpdateClient() {
        return updateClient;
    }

    public CompletableFuture<QueryResponse> requestAsync(QueryRequest request) {
        if (cloud) {
            CloudHttp2SolrClient cloudHttp2SolrClient = (CloudHttp2SolrClient) client;

            // Get the Solr endpoints
            Collection<Slice> activeSlices =
                    cloudHttp2SolrClient
                            .getClusterState()
                            .getCollection(collection)
                            .getActiveSlices();

            List<LBSolrClient.Endpoint> endpoints = new ArrayList<>();
            for (Slice slice : activeSlices) {
                for (Replica replica : slice.getReplicas()) {
                    if (replica.getState() == Replica.State.ACTIVE) {
                        endpoints.add(new LBSolrClient.Endpoint(replica.getBaseUrl(), collection));
                    }
                }
            }

            // Shuffle the endpoints for basic load balancing
            Collections.shuffle(endpoints);

            // Get the async client
            LBHttp2SolrClient lbHttp2SolrClient = cloudHttp2SolrClient.getLbClient();
            LBSolrClient.Req req = new LBSolrClient.Req(request, endpoints);

            return lbHttp2SolrClient
                    .requestAsync(req)
                    .thenApply(rsp -> new QueryResponse(rsp.getResponse(), lbHttp2SolrClient));
        } else {
            return ((Http2SolrClient) client)
                    .requestAsync(request)
                    .thenApply(nl -> new QueryResponse(nl, client));
        }
    }

    public static SolrConnection getConnection(Map<String, Object> stormConf, String boltType) {
        collection = ConfUtils.getString(stormConf, "solr." + boltType + ".collection", null);
        String zkHost = ConfUtils.getString(stormConf, "solr." + boltType + ".zkhost", null);

        String solrUrl = ConfUtils.getString(stormConf, "solr." + boltType + ".url", null);
        int queueSize = ConfUtils.getInt(stormConf, "solr." + boltType + ".queueSize", 100);

        if (StringUtils.isNotBlank(zkHost)) {
            cloud = true;

            CloudHttp2SolrClient.Builder builder =
                    new CloudHttp2SolrClient.Builder(Collections.singletonList(zkHost));

            if (StringUtils.isNotBlank(collection)) {
                builder.withDefaultCollection(collection);
            }

            CloudHttp2SolrClient cloudHttp2SolrClient = builder.build();

            return new SolrConnection(cloudHttp2SolrClient, cloudHttp2SolrClient);

        } else if (StringUtils.isNotBlank(solrUrl)) {
            cloud = false;

            Http2SolrClient http2SolrClient = new Http2SolrClient.Builder(solrUrl).build();

            ConcurrentUpdateHttp2SolrClient concurrentUpdateHttp2SolrClient =
                    new ConcurrentUpdateHttp2SolrClient.Builder(solrUrl, http2SolrClient, true)
                            .withQueueSize(queueSize)
                            .build();

            return new SolrConnection(http2SolrClient, concurrentUpdateHttp2SolrClient);

        } else {
            throw new RuntimeException("SolrClient should have zk or solr URL set up");
        }
    }

    public void close() throws IOException, SolrServerException {
        if (updateClient != null) {
            client.commit();
            updateClient.commit();
            updateClient.close();
        }
    }
}
