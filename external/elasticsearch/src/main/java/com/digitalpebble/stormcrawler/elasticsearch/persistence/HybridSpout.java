package com.digitalpebble.stormcrawler.elasticsearch.persistence;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

import java.time.Instant;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.persistence.EmptyQueueListener;
import com.digitalpebble.stormcrawler.util.ConfUtils;

public class HybridSpout extends AggregationSpout
        implements EmptyQueueListener {

    private static final Logger LOG = LoggerFactory
            .getLogger(HybridSpout.class);

    protected static final String RELOADPARAMNAME = "es.status.max.urls.per.reload";

    private int bufferReloadSize = 10;

    @Override
    public void open(Map stormConf, TopologyContext context,
            SpoutOutputCollector collector) {
        super.open(stormConf, context, collector);
        bufferReloadSize = ConfUtils.getInt(stormConf, RELOADPARAMNAME,
                maxURLsPerBucket);
        buffer.setEmptyQueueListener(this);
    }

    @Override
    public void emptyQueue(String queueName) {

        LOG.info("Emptied buffer queue for {}", queueName);

        if (!currentBuckets.contains(queueName)) {
            // not interested in this one any more
            return;
        }

        LOG.info("Querying for more docs for {}", queueName);

        if (queryDate == null) {
            queryDate = new Date();
            lastTimeResetToNOW = Instant.now();
        }

        String formattedQueryDate = ISODateTimeFormat.dateTimeNoMillis()
                .print(queryDate.getTime());

        BoolQueryBuilder queryBuilder = boolQuery().filter(QueryBuilders
                .rangeQuery("nextFetchDate").lte(formattedQueryDate));

        queryBuilder.filter(QueryBuilders.termQuery(partitionField, queueName));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(queryBuilder);
        sourceBuilder.from(0);
        sourceBuilder.size(bufferReloadSize);
        sourceBuilder.explain(false);
        sourceBuilder.trackTotalHits(false);

        // sort within a bucket
        if (StringUtils.isNotBlank(bucketSortField)) {
            FieldSortBuilder sorter = SortBuilders.fieldSort(bucketSortField)
                    .order(SortOrder.ASC);
            sourceBuilder.sort(sorter);
        }

        SearchRequest request = new SearchRequest(indexName);

        request.source(sourceBuilder);

        // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-preference.html
        // _shards:2,3
        if (shardID != -1) {
            request.preference("_shards:" + shardID);
        }

        // dump query to log
        LOG.debug("{} ES query {} - {}", logIdprefix, queueName,
                request.toString());

        client.searchAsync(request, RequestOptions.DEFAULT, this);
    }

    /**
     * gets the results for a specific host
     */
    public void onResponse(SearchResponse response) {

        // aggregations? process with the super class
        if (response.getAggregations() != null) {
            super.onResponse(response);
            return;
        }

        int alreadyprocessed = 0;
        int numDocs = 0;

        SearchHit[] hits = response.getHits().getHits();

        for (SearchHit hit : hits) {
            numDocs++;
            if (!addHitToBuffer(hit)) {
                alreadyprocessed++;
            }
        }

        eventCounter.scope("ES_queries").incrBy(1);
        eventCounter.scope("ES_docs").incrBy(numDocs);
        eventCounter.scope("already_being_processed").incrBy(alreadyprocessed);

        LOG.info(
                "{} ES term query returned {} hits  in {} msec with {} already being processed.",
                logIdprefix, numDocs, response.getTook().getMillis(),
                alreadyprocessed);
    }

    /**
     * A failure caused by an exception at some phase of the task.
     */
    public void onFailure(Exception e) {
        LOG.error("Exception with ES query", e);
    }

}
