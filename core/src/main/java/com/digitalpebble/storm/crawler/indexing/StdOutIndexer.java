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

package com.digitalpebble.storm.crawler.indexing;

import java.util.Iterator;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.digitalpebble.storm.crawler.Metadata;

/**
 * Indexer which generates fields for indexing and sends them to the standard
 * output. Useful for debugging and as an illustration of what
 * AbstractIndexerBolt provides.
 */
@SuppressWarnings("serial")
public class StdOutIndexer extends AbstractIndexerBolt {
    OutputCollector _collector;

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map conf, TopologyContext context,
            OutputCollector collector) {
        super.prepare(conf, context, collector);
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url");
        Metadata metadata = (Metadata) tuple.getValueByField("metadata");
        String text = tuple.getStringByField("text");

        // TODO binary content?

        // should this document be kept?
        boolean keep = filterDocument(metadata);
        if (!keep) {
            _collector.ack(tuple);
            return;
        }

        // display text of the document?
        if (fieldNameForText() != null) {
            System.out.println(fieldNameForText() + "\t" + trimValue(text));
        }

        if (fieldNameForURL() != null) {
            System.out.println(fieldNameForURL() + "\t" + trimValue(url));
        }

        // which metadata to display?
        Map<String, String[]> keyVals = filterMetadata(metadata);

        Iterator<String> iterator = keyVals.keySet().iterator();
        while (iterator.hasNext()) {
            String fieldName = iterator.next();
            String[] values = keyVals.get(fieldName);
            for (String value : values) {
                System.out.println(fieldName + "\t" + trimValue(value));
            }
        }
        _collector.ack(tuple);
    }

    private String trimValue(String value) {
        if (value.length() > 100)
            return value.length() + " chars";
        return value;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}