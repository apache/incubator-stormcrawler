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
package com.digitalpebble.stormcrawler.opensearch.parse.filter;

import com.digitalpebble.stormcrawler.JSONResource;
import com.digitalpebble.stormcrawler.opensearch.OpensearchConnection;
import com.digitalpebble.stormcrawler.parse.ParseFilter;
import com.digitalpebble.stormcrawler.parse.ParseResult;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import org.jetbrains.annotations.NotNull;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

/**
 * Wraps a ParseFilter whose resources are in a JSON file that can be stored in ES. The benefit of
 * doing this is that the resources can be refreshed automatically and modified without having to
 * recompile the jar and restart the topology. The connection to ES is done via the config and uses
 * a new bolt type 'config'.
 *
 * <p>The configuration of the delegate is done in the parsefilters.json as usual.
 *
 * <pre>
 *  {
 *     "class": "com.digitalpebble.stormcrawler.elasticsearch.parse.filter.JSONResourceWrapper",
 *     "name": "ESCollectionTagger",
 *     "params": {
 *         "refresh": "60",
 *         "delegate": {
 *             "class": "com.digitalpebble.stormcrawler.parse.filter.CollectionTagger",
 *             "params": {
 *                 "file": "collections.json"
 *             }
 *         }
 *     }
 *  }
 * </pre>
 *
 * The resource file can be pushed to ES with
 *
 * <pre>
 *  curl -XPUT "$ESHOST/config/_create/collections.json" -H 'Content-Type: application/json' -d @src/main/resources/collections.json
 * </pre>
 */
public class JSONResourceWrapper extends ParseFilter {

    private static final Logger LOG = LoggerFactory.getLogger(JSONResourceWrapper.class);

    private ParseFilter delegatedParseFilter;

    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode filterParams) {

        String parsefilterclass = null;

        JsonNode delegateNode = filterParams.get("delegate");
        if (delegateNode == null) {
            throw new RuntimeException("delegateNode undefined!");
        }

        JsonNode node = delegateNode.get("class");
        if (node != null && node.isTextual()) {
            parsefilterclass = node.asText();
        }

        if (parsefilterclass == null) {
            throw new RuntimeException("parsefilter.class undefined!");
        }

        // load an instance of the delegated parsefilter
        try {
            Class<?> filterClass = Class.forName(parsefilterclass);

            boolean subClassOK = ParseFilter.class.isAssignableFrom(filterClass);
            if (!subClassOK) {
                throw new RuntimeException(
                        "Filter " + parsefilterclass + " does not extend ParseFilter");
            }

            delegatedParseFilter = (ParseFilter) filterClass.getDeclaredConstructor().newInstance();

            // check that it implements JSONResource
            if (!JSONResource.class.isInstance(delegatedParseFilter)) {
                throw new RuntimeException(
                        "Filter " + parsefilterclass + " does not implement JSONResource");
            }

        } catch (Exception e) {
            LOG.error("Can't setup {}: {}", parsefilterclass, e);
            throw new RuntimeException("Can't setup " + parsefilterclass, e);
        }

        // configure it
        node = delegateNode.get("params");

        delegatedParseFilter.configure(stormConf, node);

        int refreshRate = 600;

        node = filterParams.get("refresh");
        if (node != null && node.isInt()) {
            refreshRate = node.asInt(refreshRate);
        }

        final JSONResource resource = (JSONResource) delegatedParseFilter;

        new Timer()
                .schedule(
                        new TimerTask() {
                            private RestHighLevelClient esClient;

                            public void run() {
                                if (esClient == null) {
                                    try {
                                        esClient =
                                                OpensearchConnection.getClient(stormConf, "config");
                                    } catch (Exception e) {
                                        LOG.error("Exception while creating ES connection", e);
                                    }
                                }
                                if (esClient != null) {
                                    LOG.info("Reloading json resources from ES");
                                    try {
                                        GetResponse response =
                                                esClient.get(
                                                        new GetRequest(
                                                                "config",
                                                                resource.getResourceFile()),
                                                        RequestOptions.DEFAULT);
                                        resource.loadJSONResources(
                                                new ByteArrayInputStream(
                                                        response.getSourceAsBytes()));
                                    } catch (Exception e) {
                                        LOG.error("Can't load config from ES", e);
                                    }
                                }
                            }
                        },
                        0,
                        refreshRate * 1000);
    }

    @Override
    public void filter(String URL, byte[] content, DocumentFragment doc, ParseResult parse) {
        delegatedParseFilter.filter(URL, content, doc, parse);
    }
}
