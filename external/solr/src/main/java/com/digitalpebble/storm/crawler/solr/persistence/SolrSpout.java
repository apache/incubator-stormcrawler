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

package com.digitalpebble.storm.crawler.solr.persistence;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.solr.SolrConnection;
import com.digitalpebble.storm.crawler.util.ConfUtils;
import com.digitalpebble.storm.crawler.util.URLPartitioner;

public class SolrSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory.getLogger(SolrSpout.class);

    private static final String BOLT_TYPE = "status";

    private static final String SolrIndexCollection = "solr.status.collection";
    private static final String SolrMaxInflightParam = "solr.status.max.inflight.urls.per.bucket";
    private static final String SolrDiversityFieldParam = "solr.status.bucket.field";
    private static final String SolrDiversityBucketParam = "solr.status.bucket.maxsize";

    private String collection;

    private SpoutOutputCollector _collector;

    private SolrConnection connection;

    private final int bufferSize = 100;

    private Queue<Values> buffer = new LinkedList<Values>();

    private int lastStartOffset = 0;

    private URLPartitioner partitioner;

    private int maxInFlightURLsPerBucket = -1;

    private String diversityField = null;

    private int diversityBucketSize = 0;

    /** Keeps a count of the URLs being processed per host/domain/IP **/
    private Map<String, Integer> inFlightTracker = new HashMap<String, Integer>();

    // URL / politeness bucket (hostname / domain etc...)
    private Map<String, String> beingProcessed = new HashMap<String, String>();

    @Override
    public void open(Map stormConf, TopologyContext context,
            SpoutOutputCollector collector) {
        collection = ConfUtils.getString(stormConf, SolrIndexCollection,
                "status");
        maxInFlightURLsPerBucket = ConfUtils.getInt(stormConf,
                SolrMaxInflightParam, 1);

        diversityField = ConfUtils.getString(stormConf, SolrDiversityFieldParam);
        diversityBucketSize = ConfUtils.getInt(stormConf, SolrDiversityBucketParam, 100);

        try {
            connection = SolrConnection.getConnection(stormConf, BOLT_TYPE);
        } catch (Exception e) {
            LOG.error("Can't connect to Solr: {}", e);
            throw new RuntimeException(e);
        }

        partitioner = new URLPartitioner();
        partitioner.configure(stormConf);

        _collector = collector;
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

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "metadata"));
    }

    @Override
    public void nextTuple() {
        // have anything in the buffer?
        if (!buffer.isEmpty()) {
            Values fields = buffer.remove();
            String url = fields.get(0).toString();
            Metadata metadata = (Metadata) fields.get(1);

            String partitionKey = partitioner.getPartition(url, metadata);

            // check whether we already have too tuples in flight for this
            // partition key

            if (maxInFlightURLsPerBucket != -1) {
                Integer inflightforthiskey = inFlightTracker.get(partitionKey);
                if (inflightforthiskey == null)
                    inflightforthiskey = new Integer(0);
                if (inflightforthiskey.intValue() >= maxInFlightURLsPerBucket) {
                    // do it later! left it out of the queue for now
                    return;
                }
                int currentCount = inflightforthiskey.intValue();
                inFlightTracker.put(partitionKey, ++currentCount);
            }

            beingProcessed.put(url, partitionKey);

            this._collector.emit(fields, url);
            return;
        }

        // re-populate the buffer
        populateBuffer();
    }

    private void populateBuffer() {
        // TODO Sames as the ElasticSearchSpout?
        //  TODO Use the cursor feature?
        // https://cwiki.apache.org/confluence/display/solr/Pagination+of+Results
        SolrQuery query = new SolrQuery();

        query.setQuery("*:*").addFilterQuery("nextFetchDate:[* TO NOW]")
                .setStart(lastStartOffset).setRows(this.bufferSize);

        if (StringUtils.isNotBlank(diversityField)) {
            query.addFilterQuery(String.format("{!collapse field=%s}", diversityField));
            query.set("expand", "true").set("expand.rows", diversityBucketSize);
        }

        try {
            QueryResponse response = connection.getClient().query(query);
            SolrDocumentList docs = new SolrDocumentList();

            int numhits = response.getResults().size();
      
            if (StringUtils.isNotBlank(diversityField)) {
              //get expand result
              Map<String, SolrDocumentList> expandedResults = response
                      .getExpandedResults();

              for (SolrDocument doc : response.getResults()) {
                String df = (String) doc.get(diversityField);

                //is there multiple urls for this host
                SolrDocumentList dfList = expandedResults.get(df);
                if (dfList != null) {
                  docs.addAll(dfList);
                } else {
                  docs.add(doc);
                }
              }
            } else {
              docs = response.getResults();
            }

            // no more results?
            if (numhits == 0)
                lastStartOffset = 0;
            else
                lastStartOffset += numhits;

            for (SolrDocument doc : docs) {
                String url = (String) doc.get("url");

                // is already being processed - skip it!
                if (beingProcessed.containsKey(url))
                    continue;

                Metadata metadata = new Metadata();

                String mdAsString = (String) doc.get("metadata");
                // get the serialized metadata information
                if (mdAsString != null) {
                    // parse the string and generate the MD accordingly
                    // url.path: http://www.lemonde.fr/
                    // depth: 1
                    String[] kvs = mdAsString.split("\n");
                    for (String pair : kvs) {
                        String[] kv = pair.split(": ");
                        if (kv.length != 2) {
                            LOG.info("Invalid key value pair {}", pair);
                            continue;
                        }
                        metadata.addValue(kv[0], kv[1]);
                    }
                }

                buffer.add(new Values(url, metadata));
            }

        } catch (Exception e) {
            LOG.error("Can't query Solr: {}", e);
        }
    }

    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
        String partitionKey = beingProcessed.remove(msgId);
        decrementPartitionKey(partitionKey);
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
        String partitionKey = beingProcessed.remove(msgId);
        decrementPartitionKey(partitionKey);
    }

    private void decrementPartitionKey(String partitionKey) {
        if (partitionKey == null)
            return;
        Integer currentValue = this.inFlightTracker.get(partitionKey);
        if (currentValue == null)
            return;
        int currentVal = currentValue.intValue();
        currentVal--;
        this.inFlightTracker.put(partitionKey, currentVal);
    }
}
