/*
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

package com.digitalpebble.storm.crawler.indexer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;

import org.junit.After;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.TestOutputCollector;
import com.digitalpebble.storm.crawler.TestUtil;
import com.digitalpebble.storm.crawler.indexing.AbstractIndexerBolt;

public class IndexerTester {
    AbstractIndexerBolt bolt;
    protected TestOutputCollector output;

    protected void setupIndexerBolt(AbstractIndexerBolt bolt) {
        this.bolt = bolt;
        output = new TestOutputCollector();
    }

    @After
    public void cleanupBolt() {
        if (bolt != null) {
            bolt.cleanup();
        }
        output = null;
    }

    protected void prepareIndexerBolt(Map config) {
        bolt.prepare(config, TestUtil.getMockedTopologyContext(),
                new OutputCollector(output));
    }

    protected void index(String url, Metadata metadata) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        bolt.execute(tuple);
    }

    protected void index(String url, String content, Metadata metadata)
            throws IOException {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getBinaryByField("content")).thenReturn(content.getBytes());
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        bolt.execute(tuple);
    }
}
