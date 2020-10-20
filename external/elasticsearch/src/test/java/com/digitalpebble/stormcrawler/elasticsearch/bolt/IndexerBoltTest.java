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

package com.digitalpebble.stormcrawler.elasticsearch.bolt;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import com.digitalpebble.stormcrawler.Constants;
import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.TestOutputCollector;
import com.digitalpebble.stormcrawler.TestUtil;
import com.digitalpebble.stormcrawler.indexing.AbstractIndexerBolt;

public class IndexerBoltTest {

    private ElasticsearchContainer container;
    private IndexerBolt bolt;
    protected TestOutputCollector output;

    private static final Logger LOG = LoggerFactory
            .getLogger(IndexerBoltTest.class);

    @Before
    public void setupIndexerBolt() {

        String version = System.getProperty("elasticsearch-version");
        if (version == null)
            version = "7.5.0";
        LOG.info("Starting docker instance of Elasticsearch {}...", version);

        container = new ElasticsearchContainer(
                "docker.elastic.co/elasticsearch/elasticsearch:" + version);
        container.start();

        bolt = new IndexerBolt("content");

        // give the indexer the port for connecting to ES

        Map conf = new HashMap();
        conf.put(AbstractIndexerBolt.urlFieldParamName, "url");
        conf.put(AbstractIndexerBolt.canonicalMetadataParamName, "canonical");
        conf.put("es.indexer.addresses", container.getHttpHostAddress());

        output = new TestOutputCollector();

        bolt.prepare(conf, TestUtil.getMockedTopologyContext(),
                new OutputCollector(output));
    }

    @After
    public void close() {
        LOG.info("Closing indexer bolt and ES container");
        bolt.cleanup();
        container.close();
        output = null;
    }

    private void index(String url, String text, Metadata metadata) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getStringByField("text")).thenReturn(text);
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        bolt.execute(tuple);
    }

    @Test
    // https://github.com/DigitalPebble/storm-crawler/issues/832
    public void simultaneousCanonicals() {

        Metadata m1 = new Metadata();
        String url = "https://www.obozrevatel.com/ukr/dnipro/city/u-dnipri-ta-oblasti-ogolosili-shtormove-poperedzhennya.htm";
        m1.addValue("canonical", url);

        Metadata m2 = new Metadata();
        String url2 = "https://www.obozrevatel.com/ukr/dnipro/city/u-dnipri-ta-oblasti-ogolosili-shtormove-poperedzhennya/amp.htm";
        m2.addValue("canonical", url);

        index(url, "", m1);
        index(url2, "", m2);

        // check that something has been emitted out
        while (output.getEmitted(Constants.StatusStreamName).size() == 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // should be two in status output
        assertEquals(2, output.getEmitted(Constants.StatusStreamName).size());

        // and 2 acked
        assertEquals(2, output.getAckedTuples().size());

        // TODO check output in ES?

    }

}