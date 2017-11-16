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

package com.digitalpebble.stormcrawler.elasticsearch.util;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.elasticsearch.ElasticSearchConnection;
import com.digitalpebble.stormcrawler.util.ConfUtils;

/**
 * Extracts the URLs (and possibly metadata) from a status or doc index into a
 * file.
 **/

public class URLExtractor {

    private static final Logger LOG = LoggerFactory
            .getLogger(URLExtractor.class);

    // indexer or status
    private String boltType;

    private Client client;

    private int cumulated = 0;

    private BufferedOutputStream output = null;

    private String indexName;

    private String docType;

    URLExtractor(Map stormConf, String outfile, String boltType)
            throws FileNotFoundException, UnknownHostException {

        this.output = new BufferedOutputStream(new FileOutputStream(new File(
                outfile)));

        this.boltType = boltType;

        this.client = ElasticSearchConnection.getClient(stormConf, boltType);

        this.indexName = ConfUtils.getString(stormConf, "es." + boltType
                + ".index.name", "status");

        this.docType = ConfUtils.getString(stormConf, "es." + boltType
                + ".doc.type", "status");
    }

    public static void main(String[] args) throws IOException {

        if (args.length < 3) {
            LOG.error("Usage: URLExtractor <CONF_FILE> <OUTFILE> [indexer|status]");
            System.exit(-1);
        }

        String confFile = args[0];
        String outfile = args[1];
        String boltType = args[2];

        // load the conf
        Config conf = new Config();
        ConfUtils.loadConf(confFile, conf);

        URLExtractor gen = new URLExtractor(conf, outfile, boltType);

        gen.queryES();

        gen.output.close();

        gen.client.close();

        LOG.info("Total : {}", gen.cumulated);
    }

    private final void queryES() throws IOException {
        int maxBufferSize = 100;

        SearchResponse scrollResp = client.prepareSearch(this.indexName)
                .setTypes(this.docType).setScroll(new TimeValue(60000))
                .setQuery(QueryBuilders.matchAllQuery()).setSize(maxBufferSize)
                .execute().actionGet();

        long total = scrollResp.getHits().getTotalHits();

        LOG.info("Total hits found {}", total);

        // Scroll until no hits are returned
        while (true) {
            SearchHits hits = scrollResp.getHits();
            LOG.info("Processing {} documents - {} out of {}",
                    hits.getHits().length, cumulated, total);
            for (SearchHit hit : hits) {
                String url = null;

                Map<String, Object> sourceMap = hit.getSourceAsMap();
                if (sourceMap == null) {
                    hit.getFields().get("url");
                } else {
                    url = sourceMap.get("url").toString();
                }

                if (StringUtils.isBlank(url)) {
                    LOG.error("Can't retrieve URL for hit {}", hit);
                    continue;
                }

                StringBuilder line = new StringBuilder(url);

                if (boltType.equalsIgnoreCase("status")) {
                    sourceMap = (Map<String, Object>) sourceMap.get("metadata");
                    if (sourceMap != null) {
                        Iterator<Entry<String, Object>> iter = sourceMap
                                .entrySet().iterator();
                        while (iter.hasNext()) {
                            Entry<String, Object> e = iter.next();
                            Object o = e.getValue();
                            if (o == null) {
                                continue;
                            }
                            if (o instanceof String) {
                                line.append("\t").append(e.getKey())
                                        .append("=").append(o);
                            }
                            if (o instanceof List) {
                                for (Object val : (List) o) {
                                    line.append("\t").append(e.getKey())
                                            .append("=").append(val.toString());
                                }
                            }
                        }
                    }
                }

                line.append("\n");
                IOUtils.write(line.toString(), output, "UTF-8");
                cumulated++;
            }
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(600000)).execute().actionGet();
            // Break condition: No hits are returned
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }
    }

}