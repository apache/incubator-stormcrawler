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
package com.digitalpebble.stormcrawler.persistence;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.metric.api.IMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import com.digitalpebble.stormcrawler.Constants;
import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.MetadataTransfer;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Abstract bolt used to store the status of URLs. Uses the DefaultScheduler and
 * MetadataTransfer.
 **/
@SuppressWarnings("serial")
public abstract class AbstractStatusUpdaterBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory
            .getLogger(AbstractStatusUpdaterBolt.class);

    private static final DateFormat dateFormat = new SimpleDateFormat(
            "yyyy-MM-dd'T'HH:mm:ss");

    /**
     * Parameter name to indicate whether the internal cache should be used for
     * discovered URLs. The value of the parameter is a boolean - true by
     * default.
     **/
    public static String useCacheParamName = "status.updater.use.cache";

    /** Number of successive FETCH_ERROR before status changes to ERROR **/
    public static String maxFetchErrorsParamName = "max.fetch.errors";

    /**
     * Parameter name to configure the cache @see
     * http://docs.guava-libraries.googlecode
     * .com/git/javadoc/com/google/common/cache/CacheBuilderSpec.html Default
     * value is "maximumSize=10000,expireAfterAccess=1h"
     **/
    public static String cacheConfigParamName = "status.updater.cache.spec";

    protected OutputCollector _collector;

    private Scheduler scheduler;
    private MetadataTransfer mdTransfer;

    private Cache<Object, Object> cache;
    private boolean useCache = true;

    private int maxFetchErrors = 3;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        _collector = collector;

        scheduler = Scheduler.getInstance(stormConf);

        mdTransfer = MetadataTransfer.getInstance(stormConf);

        useCache = ConfUtils.getBoolean(stormConf, useCacheParamName, true);

        if (useCache) {
            String spec = ConfUtils.getString(stormConf, cacheConfigParamName);
            cache = CacheBuilder.from(spec).build();

            context.registerMetric("cache size", new IMetric() {
                @Override
                public Object getValueAndReset() {
                    return cache.size();
                }
            }, 30);
        }

        maxFetchErrors = ConfUtils
                .getInt(stormConf, maxFetchErrorsParamName, 3);
    }

    @Override
    public void execute(Tuple tuple) {

        String url = tuple.getStringByField("url");
        Status status = (Status) tuple.getValueByField("status");

        boolean potentiallyNew = status.equals(Status.DISCOVERED);

        // if the URL is a freshly discovered one
        // check whether it is already known in the cache
        // if so we've already seen it and don't need to
        // store it again
        if (potentiallyNew && useCache) {
            if (cache.getIfPresent(url) != null) {
                // no need to add it to the queue
                LOG.debug("URL {} already in cache", url);
                _collector.ack(tuple);
                return;
            } else {
                LOG.debug("URL {} not in cache", url);
            }
        }

        Metadata metadata = (Metadata) tuple.getValueByField("metadata");

        // store last processed or discovery date
        final String nowAsString = dateFormat.format(new Date());
        if (status.equals(Status.DISCOVERED)) {
            metadata.setValue("discoveryDate", nowAsString);
        } else {
            metadata.setValue("lastProcessedDate", nowAsString);
        }

        // too many fetch errors?
        if (status.equals(Status.FETCH_ERROR)) {
            String errorCount = metadata
                    .getFirstValue(Constants.fetchErrorCountParamName);
            int count = 0;
            try {
                count = Integer.parseInt(errorCount);
            } catch (NumberFormatException e) {
            }
            count++;
            if (count == maxFetchErrors) {
                status = Status.ERROR;
                metadata.setValue(Constants.STATUS_ERROR_CAUSE,
                        "maxFetchErrors");
            } else {
                metadata.setValue(Constants.fetchErrorCountParamName,
                        Integer.toString(count));
            }
        }

        // delete any existing error count metadata
        // e.g. status changed
        if (!status.equals(Status.FETCH_ERROR)) {
            metadata.remove(Constants.fetchErrorCountParamName);
        }

        // determine the value of the next fetch based on the status
        Date nextFetch = scheduler.schedule(status, metadata);

        // filter metadata just before storing it, so that non-persisted
        // metadata is available to fetch schedulers
        metadata = mdTransfer.filter(metadata);

        // extensions of this class will handle the storage
        // on a per document basis

        try {
            store(url, status, metadata, nextFetch);
        } catch (Exception e) {
            LOG.error("Exception caught when storing", e);
            _collector.fail(tuple);
            return;
        }

        ack(tuple, url);
    }

    /**
     * Must be overridden for implementations where the actual writing can be
     * delayed e.g. put in a buffer
     **/
    protected void ack(Tuple t, String url) {
        // keep the URL in the cache
        if (useCache) {
            cache.put(url, "");
        }

        _collector.ack(t);
    }

    protected abstract void store(String url, Status status, Metadata metadata,
            Date nextFetch) throws Exception;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}