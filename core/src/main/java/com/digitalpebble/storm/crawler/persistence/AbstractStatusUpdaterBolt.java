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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.util.MetadataTransfer;

/**
 * Abstract bolt used to store the status of URLs. Uses the DefaultScheduler and
 * MetadataTransfer.
 **/
public abstract class AbstractStatusUpdaterBolt extends BaseRichBolt {

    protected OutputCollector _collector;

    private DefaultScheduler scheduler;
    private MetadataTransfer mdTransfer;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        _collector = collector;

        scheduler = new DefaultScheduler();
        scheduler.init(stormConf);
        mdTransfer = new MetadataTransfer(stormConf);
    }

    @Override
    public void execute(Tuple tuple) {

        String url = tuple.getStringByField("url");
        Status status = (Status) tuple.getValueByField("status");

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

        _collector.ack(tuple);
    }

    public abstract void store(String url, Status status, Metadata metadata,
            Date nextFetch) throws Exception;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
