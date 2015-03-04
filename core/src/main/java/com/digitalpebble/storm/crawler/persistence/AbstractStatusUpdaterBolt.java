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
package com.digitalpebble.storm.crawler.persistence;

import java.util.Date;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.util.ConfUtils;
import com.digitalpebble.storm.crawler.util.MetadataTransfer;
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

    /**
     * Parameter name to indicate whether the internal cache should be used for
     * discovered URLs. The value of the parameter is a boolean - true by
     * default.
     **/
    public static String useCacheParamName = "status.updater.use.cache";

    /**
     * Parameter name to configure the cache @see
     * http://docs.guava-libraries.googlecode
     * .com/git/javadoc/com/google/common/cache/CacheBuilderSpec.html Default
     * value is "maximumSize=10000,expireAfterAccess=1h"
     **/
    public static String cacheConfigParamName = "status.updater.cache.spec";

    protected OutputCollector _collector;

    private DefaultScheduler scheduler;
    private MetadataTransfer mdTransfer;

    private Cache<Object, Object> cache;
    private boolean useCache = true;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        _collector = collector;

        scheduler = new DefaultScheduler();
        scheduler.init(stormConf);
        mdTransfer = MetadataTransfer.getInstance(stormConf);

        useCache = ConfUtils.getBoolean(stormConf, useCacheParamName, true);

        if (useCache) {
            String spec = "maximumSize=10000,expireAfterAccess=1h";
            spec = ConfUtils.getString(stormConf, cacheConfigParamName, spec);
            cache = CacheBuilder.from(spec).build();
        }
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
        metadata = mdTransfer.filter(metadata);

        // determine the value of the next fetch based on the status
        Date nextFetch = scheduler.schedule(status, metadata);

        // extensions of this class will handle the storage
        // on a per document basis

        try {
            store(url, status, metadata, nextFetch);
        } catch (Exception e) {
            _collector.fail(tuple);
            return;
        }

        // keep the URL in the cache
        if (useCache) {
            cache.put(url, status);
        }

        _collector.ack(tuple);
    }

    public abstract void store(String url, Status status, Metadata metadata,
            Date nextFetch) throws Exception;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
