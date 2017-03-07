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

package com.digitalpebble.stormcrawler.bolt;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.filtering.URLFilters;
import com.digitalpebble.stormcrawler.persistence.Status;

public class URLFilterBolt extends BaseRichBolt {

    private URLFilters urlFilters;

    protected OutputCollector collector;

    private static final String _s = com.digitalpebble.stormcrawler.Constants.StatusStreamName;

    @Override
    public void execute(Tuple input) {
        // the input can come from the standard stream or the status one
        // we'll emit to whichever it came from
        String stream = input.getSourceStreamId();
        // must have at least a URL and metadata, possibly a status
        String urlString = input.getStringByField("url");
        Metadata metadata = (Metadata) input.getValueByField("metadata");
        Status status = (Status) input.getValueByField("status");

        String filtered = urlFilters.filter(null, null, urlString);
        if (StringUtils.isBlank(filtered)) {
            // LOG? Add counter?
            collector.ack(input);
            return;
        }
        Values v = new Values(filtered, metadata, status);
        collector.emit(stream, v);
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Fields f = new Fields("url", "metadata", "status");
        declarer.declareStream(_s, f);
        declarer.declare(f);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        this.collector = collector;
        urlFilters = URLFilters.fromConf(stormConf);
    }

}
