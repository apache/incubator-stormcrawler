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
package com.digitalpebble.stormcrawler.solr.persistence;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.AbstractQueryingSpout;
import com.digitalpebble.stormcrawler.solr.SolrConnection;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import java.time.Instant;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrSpout extends AbstractQueryingSpout {

    private static final Logger LOG = LoggerFactory.getLogger(SolrSpout.class);

    private static final String BOLT_TYPE = "status";

    private static final String SolrDiversityFieldParam = "solr.status.bucket.field";
    private static final String SolrDiversityBucketParam = "solr.status.bucket.maxsize";
    private static final String SolrMetadataPrefix = "solr.status.metadata.prefix";
    private static final String SolrMaxResultsParam = "solr.status.max.results";

    private SolrConnection connection;

    private int maxNumResults = 10;

    private int lastStartOffset = 0;

    private Instant lastNextFetchDate = null;

    private String diversityField = null;

    private int diversityBucketSize = 0;

    private String mdPrefix;

    @Override
    public void open(
            Map<String, Object> stormConf,
            TopologyContext context,
            SpoutOutputCollector collector) {

        super.open(stormConf, context, collector);

        // This implementation works only where there is a single instance
        // of the spout. Having more than one instance means that they would run
        // the same queries and send the same tuples down the topology.

        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        if (totalTasks > 1) {
            throw new RuntimeException("Can't have more than one instance of SOLRSpout");
        }

        diversityField = ConfUtils.getString(stormConf, SolrDiversityFieldParam);
        diversityBucketSize = ConfUtils.getInt(stormConf, SolrDiversityBucketParam, 5);
        // the results have the first hit separate from the expansions
        diversityBucketSize--;

        mdPrefix = ConfUtils.getString(stormConf, SolrMetadataPrefix, "metadata");

        maxNumResults = ConfUtils.getInt(stormConf, SolrMaxResultsParam, 10);

        try {
            connection = SolrConnection.getConnection(stormConf, BOLT_TYPE);
        } catch (Exception e) {
            LOG.error("Can't connect to Solr: {}", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                LOG.error("Can't close connection to Solr: {}", e);
            }
        }
    }

    protected void populateBuffer() {

        SolrQuery query = new SolrQuery();

        if (lastNextFetchDate == null) {
            lastNextFetchDate = Instant.now();
            lastStartOffset = 0;
            lastTimeResetToNOW = Instant.now();
        }
        // reset the value for next fetch date if the previous one is too
        // old
        else if (resetFetchDateAfterNSecs != -1) {
            Instant changeNeededOn =
                    Instant.ofEpochMilli(
                            lastTimeResetToNOW.toEpochMilli() + (resetFetchDateAfterNSecs * 1000));
            if (Instant.now().isAfter(changeNeededOn)) {
                LOG.info(
                        "lastDate reset based on resetFetchDateAfterNSecs {}",
                        resetFetchDateAfterNSecs);
                lastNextFetchDate = Instant.now();
                lastStartOffset = 0;
            }
        }

        query.setQuery("*:*")
                .addFilterQuery("nextFetchDate:[* TO " + lastNextFetchDate + "]")
                .setStart(lastStartOffset)
                .setRows(this.maxNumResults);

        if (StringUtils.isNotBlank(diversityField) && diversityBucketSize > 0) {
            query.addFilterQuery(
                    String.format("{!collapse field=%s sort='nextFetchDate asc'}", diversityField));
            query.set("expand", "true").set("expand.rows", diversityBucketSize);
            query.set("expand.sort", "nextFetchDate asc");
        }

        LOG.debug("QUERY => {}", query.toString());

        try {
            long startQuery = System.currentTimeMillis();
            QueryResponse response = connection.getClient().query(query);
            long endQuery = System.currentTimeMillis();

            queryTimes.addMeasurement(endQuery - startQuery);

            SolrDocumentList docs = new SolrDocumentList();

            LOG.debug("Response : {}", response.toString());

            // add the main results
            docs.addAll(response.getResults());

            // Add the documents collapsed by the CollapsingQParser
            Map<String, SolrDocumentList> expandedResults = response.getExpandedResults();
            if (StringUtils.isNotBlank(diversityField) && expandedResults != null) {
                for (String key : expandedResults.keySet()) {
                    docs.addAll(expandedResults.get(key));
                }
            }

            int numhits = response.getResults().size();

            // no more results?
            if (numhits == 0) {
                lastStartOffset = 0;
                lastNextFetchDate = null;
            } else {
                lastStartOffset += numhits;
            }

            String prefix = mdPrefix.concat(".");

            int alreadyProcessed = 0;
            int docReturned = 0;

            for (SolrDocument doc : docs) {
                String url = (String) doc.get("url");

                docReturned++;

                // is already being processed - skip it!
                if (beingProcessed.containsKey(url)) {
                    alreadyProcessed++;
                    continue;
                }

                Metadata metadata = new Metadata();

                Iterator<String> keyIterators = doc.getFieldNames().iterator();
                while (keyIterators.hasNext()) {
                    String key = keyIterators.next();

                    if (key.startsWith(prefix)) {
                        Collection<Object> values = doc.getFieldValues(key);

                        key = key.substring(prefix.length());
                        Iterator<Object> valueIterator = values.iterator();
                        while (valueIterator.hasNext()) {
                            String value = (String) valueIterator.next();
                            metadata.addValue(key, value);
                        }
                    }
                }

                buffer.add(url, metadata);
            }

            LOG.info(
                    "SOLR returned {} results from {} buckets in {} msec including {} already being processed",
                    docReturned,
                    numhits,
                    (endQuery - startQuery),
                    alreadyProcessed);

        } catch (Exception e) {
            LOG.error("Exception while querying Solr", e);
        }
    }

    @Override
    public void ack(Object msgId) {
        LOG.debug("Ack for {}", msgId);
        super.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        LOG.info("Fail for {}", msgId);
        super.fail(msgId);
    }
}
