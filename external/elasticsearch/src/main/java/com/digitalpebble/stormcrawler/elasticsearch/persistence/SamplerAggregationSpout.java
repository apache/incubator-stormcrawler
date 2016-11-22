package com.digitalpebble.stormcrawler.elasticsearch.persistence;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.storm.tuple.Values;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.sampler.SamplerAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;

public class SamplerAggregationSpout extends AggregationSpout {

    private static final Logger LOG = LoggerFactory
            .getLogger(SamplerAggregationSpout.class);

    @Override
    protected void populateBuffer() {

        Date now = new Date();

        LOG.info("{} Populating buffer with nextFetchDate <= {}", logIdprefix,
                now);

        QueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(
                "nextFetchDate").lte(now);

        SearchRequestBuilder srb = client.prepareSearch(indexName)
                .setTypes(docType).setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(rangeQueryBuilder).setFrom(0).setSize(0)
                .setExplain(false);

        SamplerAggregationBuilder sab = AggregationBuilders.sampler("sample")
                .field(partitionField).maxDocsPerValue(maxURLsPerBucket)
                .shardSize(maxURLsPerBucket * maxBucketNum);

        TopHitsBuilder tophits = AggregationBuilders.topHits("docs")
                .setSize(maxURLsPerBucket * maxBucketNum).setExplain(false);

        sab.subAggregation(tophits);
        srb.addAggregation(sab);

        // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-preference.html
        // _shards:2,3
        if (shardID != -1) {
            srb.setPreference("_shards:" + shardID);
        }

        // dump query to log
        LOG.debug("{} ES query {}", logIdprefix, srb.toString());

        timeStartESQuery = System.currentTimeMillis();

        isInESQuery.set(true);
        srb.execute(this);
    }

    @Override
    public void onResponse(SearchResponse response) {
        long end = System.currentTimeMillis();

        eventCounter.scope("ES_query_time_msec").incrBy(end - timeStartESQuery);

        SingleBucketAggregation agg = response.getAggregations().get("sample");

        int numhits = 0;
        int alreadyprocessed = 0;

        synchronized (buffer) {
            // filter results so that we don't include URLs we are already
            // being processed
            TopHits topHits = agg.getAggregations().get("docs");
            for (SearchHit hit : topHits.getHits().getHits()) {
                numhits++;
                Map<String, Object> keyValues = hit.sourceAsMap();
                String url = (String) keyValues.get("url");

                LOG.debug("{} -> id [{}], _source [{}]", logIdprefix,
                        hit.getId(), hit.getSourceAsString());

                // is already being processed - skip it!
                if (beingProcessed.containsKey(url)) {
                    alreadyprocessed++;
                    continue;
                }
                Metadata metadata = fromKeyValues(keyValues);
                buffer.add(new Values(url, metadata));
            }

            // Shuffle the URLs so that we don't get blocks of URLs from the
            // same
            // host or domain
            Collections.shuffle((List) buffer);
        }

        LOG.info(
                "{} ES query returned {} hits in {} msec with {} already being processed",
                logIdprefix, numhits, end - timeStartESQuery, alreadyprocessed);

        eventCounter.scope("already_being_processed").incrBy(alreadyprocessed);
        eventCounter.scope("ES_queries").incrBy(1);
        eventCounter.scope("ES_docs").incrBy(numhits);

        // remove lock
        isInESQuery.set(false);
    }

}
