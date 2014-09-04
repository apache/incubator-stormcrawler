/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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

package com.digitalpebble.storm.crawler.bolt.indexing;

import java.util.Iterator;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/** Dummy indexer which displays the fields on the std out **/

@SuppressWarnings("serial")
public class PrinterBolt extends BaseRichBolt {
    OutputCollector _collector;

    public void prepare(Map conf, TopologyContext context,
            OutputCollector collector) {
        _collector = collector;
    }

    public void execute(Tuple tuple) {
        Iterator<String> iterator = tuple.getFields().iterator();
        while (iterator.hasNext()) {
            String fieldName = iterator.next();
            Object obj = tuple.getValueByField(fieldName);

            if (obj instanceof byte[])
                System.out.println(fieldName + "\t"
                        + tuple.getBinaryByField(fieldName).length + " bytes");
            else {
                String value = tuple.getValueByField(fieldName).toString();
                if (value.length() > 100) {
                    System.out.println(fieldName + "\t" + value.length()
                            + " chars");
                } else
                    System.out.println(fieldName + "\t" + value);

            }

        }
        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}