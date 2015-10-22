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

package com.digitalpebble.storm.crawler.elasticsearch.persistence;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.IMetric;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.elasticsearch.ElasticSearchConnection;
import com.digitalpebble.storm.crawler.util.ConfUtils;
import com.digitalpebble.storm.crawler.util.URLPartitioner;

/**
 * Overly simplistic spout implementation which pulls URL from an ES index.
 * Doesn't do anything about data locality or sharding.
 **/
public class ElasticSearchSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory
            .getLogger(ElasticSearchSpout.class);

    private static final String ESBoltType = "status";

    private static final String ESStatusIndexNameParamName = "es.status.index.name";
    private static final String ESStatusDocTypeParamName = "es.status.doc.type";
    private static final String ESStatusBufferSizeParamName = "es.status.max.buffer.size";
    private static final String ESStatusMaxInflightParamName = "es.status.max.inflight.urls.per.bucket";

    private String indexName;
    private String docType;

    private SpoutOutputCollector _collector;

    private Client client;

    private int maxBufferSize = 100;

    private Queue<Values> buffer = new LinkedList<Values>();

    private int lastStartOffset = 0;
    private Date lastDate;

    private URLPartitioner partitioner;

    private int maxInFlightURLsPerBucket = -1;

    /** Keeps a count of the URLs being processed per host/domain/IP **/
    private Map<String, AtomicInteger> inFlightTracker = new HashMap<String, AtomicInteger>();

    // URL / politeness bucket (hostname / domain etc...)
    private Map<String, String> beingProcessed = new HashMap<String, String>();

    private MultiCountMetric eventCounter;

    private boolean active = true;

    @Override
    public void open(Map stormConf, TopologyContext context,
            SpoutOutputCollector collector) {

        // This implementation works only where there is a single instance
        // of the spout. Having more than one instance means that they would run
        // the same queries and send the same tuples down the topology.

        int totalTasks = context
                .getComponentTasks(context.getThisComponentId()).size();
        if (totalTasks > 1) {
            throw new RuntimeException(
                    "Can't have more than one instance of the ES spout");
        }

        indexName = ConfUtils.getString(stormConf, ESStatusIndexNameParamName,
                "status");
        docType = ConfUtils.getString(stormConf, ESStatusDocTypeParamName,
                "status");
        maxInFlightURLsPerBucket = ConfUtils.getInt(stormConf,
                ESStatusMaxInflightParamName, 1);

        maxBufferSize = ConfUtils.getInt(stormConf,
                ESStatusBufferSizeParamName, 100);

        try {
            client = ElasticSearchConnection.getClient(stormConf, ESBoltType);
        } catch (Exception e1) {
            LOG.error("Can't connect to ElasticSearch", e1);
            throw new RuntimeException(e1);
        }

        partitioner = new URLPartitioner();
        partitioner.configure(stormConf);

        _collector = collector;

        this.eventCounter = context.registerMetric("counters",
                new MultiCountMetric(), 10);

        context.registerMetric("beingProcessed", new IMetric() {
            @Override
            public Object getValueAndReset() {
                return beingProcessed.size();
            }
        }, 10);

        context.registerMetric("buffer_size", new IMetric() {
            @Override
            public Object getValueAndReset() {
                return buffer.size();
            }
        }, 10);
    }

    @Override
    public void close() {
        if (client != null)
            client.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "metadata"));
    }

    @Override
    public void nextTuple() {

        // inactive?
        if (active == false)
            return;

        // have anything in the buffer?
        if (!buffer.isEmpty()) {
            Values fields = buffer.remove();

            String url = fields.get(0).toString();
            Metadata metadata = (Metadata) fields.get(1);

            String partitionKey = partitioner.getPartition(url, metadata);

            // check whether we already have too many tuples in flight for this
            // partition key

            if (maxInFlightURLsPerBucket != -1) {
                AtomicInteger inflightforthiskey = inFlightTracker
                        .get(partitionKey);
                if (inflightforthiskey == null) {
                    inflightforthiskey = new AtomicInteger();
                    inFlightTracker.put(partitionKey, inflightforthiskey);
                } else if (inflightforthiskey.intValue() >= maxInFlightURLsPerBucket) {
                    // do it later! left it out of the queue for now
                    LOG.debug(
                            "Reached max in flight allowed ({}) for bucket {}",
                            maxInFlightURLsPerBucket, partitionKey);
                    eventCounter.scope("skipped.max.per.bucket").incrBy(1);
                    return;
                }
                inflightforthiskey.incrementAndGet();
            }

            beingProcessed.put(url, partitionKey);

            this._collector.emit(fields, url);
            eventCounter.scope("emitted").incrBy(1);

            return;
        }
        // re-populate the buffer
        populateBuffer();
    }

    /** run a query on ES to populate the internal buffer **/
    private void populateBuffer() {

        if (lastDate == null) {
            lastDate = new Date();
        }

        LOG.info("Populating buffer with nextFetchDate <= {}", lastDate);

        long start = System.currentTimeMillis();

        // TODO random sort to get better diversity of results?
        FieldSortBuilder sorter = SortBuilders.fieldSort("nextFetchDate")
                .order(SortOrder.ASC);

        // TODO use scrolls instead?
        // @see
        // https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/scrolling.html
        SearchResponse response = client
                .prepareSearch(indexName)
                .setTypes(docType)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(
                        QueryBuilders.rangeQuery("nextFetchDate").lte(lastDate))
                .addSort(sorter).setFrom(lastStartOffset)
                .setSize(maxBufferSize).setExplain(false).execute().actionGet();

        long end = System.currentTimeMillis();

        eventCounter.scope("ES_query_time_msec").incrBy(end - start);

        SearchHits hits = response.getHits();
        int numhits = hits.getHits().length;

        LOG.info("ES query returned {} hits in {} msec", numhits, (end - start));

        eventCounter.scope("ES_queries").incrBy(1);
        eventCounter.scope("ES_docs").incrBy(numhits);

        // no more results?
        if (numhits == 0) {
            lastDate = null;
            lastStartOffset = 0;
        } else {
            lastStartOffset += numhits;
        }

        // filter results so that we don't include URLs we are already
        // being processed or skip those for which we already have enough
        //
        for (int i = 0; i < hits.getHits().length; i++) {
            Map<String, Object> keyValues = hits.getHits()[i].sourceAsMap();
            String url = (String) keyValues.get("url");

            // is already being processed - skip it!
            if (beingProcessed.containsKey(url)) {
                eventCounter.scope("already_being_processed").incrBy(1);
                continue;
            }

            Map<String, List<String>> mdAsMap = (Map<String, List<String>>) keyValues
                    .get("metadata");
            Metadata metadata = new Metadata();
            if (mdAsMap != null) {
                Iterator<Entry<String, List<String>>> mdIter = mdAsMap
                        .entrySet().iterator();
                while (mdIter.hasNext()) {
                    Entry<String, List<String>> mdEntry = mdIter.next();
                    String key = mdEntry.getKey();
                    List<String> mdVals = mdEntry.getValue();
                    metadata.addValues(key, mdVals);
                }
            }
            buffer.add(new Values(url, metadata));
        }
    }

    @Override
    public void ack(Object msgId) {
        String partitionKey = beingProcessed.remove(msgId);
        decrementPartitionKey(partitionKey);
        eventCounter.scope("acked").incrBy(1);
    }

    @Override
    public void fail(Object msgId) {
        LOG.info("Fail for {}", msgId);
        String partitionKey = beingProcessed.remove(msgId);
        decrementPartitionKey(partitionKey);
        eventCounter.scope("failed").incrBy(1);
    }

    private final void decrementPartitionKey(String partitionKey) {
        if (partitionKey == null)
            return;
        AtomicInteger currentValue = this.inFlightTracker.get(partitionKey);
        if (currentValue == null)
            return;
        int newVal = currentValue.decrementAndGet();
        if (newVal == 0)
            this.inFlightTracker.remove(partitionKey);
    }

    @Override
    public void activate() {
        active = true;
    }

    @Override
    public void deactivate() {
        active = false;
    }

}
