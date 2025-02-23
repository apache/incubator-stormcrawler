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
import java.util.Map;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.stormcrawler.util.ConfUtils;

public class SolrConnection {

    private Http2SolrClient client;
    private ConcurrentUpdateHttp2SolrClient updateClient;
    

    private SolrConnection(Http2SolrClient client, ConcurrentUpdateHttp2SolrClient updateClient) {
        this.client = client;
        this.updateClient = updateClient;
    }

    public Http2SolrClient getClient() {
        return client;
    }

    public ConcurrentUpdateHttp2SolrClient getUpdateClient() {
        return updateClient;
    }

    public static SolrConnection getConnection(Map<String, Object> stormConf, String boltType) {

        String solrUrl = ConfUtils.getString(stormConf, "solr." + boltType + ".url", null);
        int queueSize = ConfUtils.getInt(stormConf, "solr." + boltType + ".queueSize", 100);

        Http2SolrClient http2SolrClient =
                    new Http2SolrClient.Builder(solrUrl)
                        .build();

        ConcurrentUpdateHttp2SolrClient updateClient =
                new ConcurrentUpdateHttp2SolrClient.Builder(solrUrl, http2SolrClient, true)
                    .withQueueSize(queueSize)
                    .build();

        return new SolrConnection(http2SolrClient, updateClient);
    }

    public void close() throws IOException, SolrServerException {
        if (client != null) {
            client.commit();
            client.close();
        }
    }
}
