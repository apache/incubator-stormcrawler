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

package com.digitalpebble.storm.crawler.bolt;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.digitalpebble.storm.crawler.Constants;
import com.digitalpebble.storm.crawler.TestOutputCollector;
import com.digitalpebble.storm.crawler.TestUtil;
import com.github.tomakehurst.wiremock.junit.WireMockRule;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

public class SimpleFetcherBoltTest extends AbstractFetcherBoltTest {

    private final static int port = 8089;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(port);

    @Before
    public void setUpContext() throws Exception {
        bolt = new SimpleFetcherBolt();
    }

    @Test
    public void test304() {

        stubFor(get(urlMatching(".+")).willReturn(aResponse().withStatus(304)));

        TestOutputCollector output = new TestOutputCollector();

        Map config = new HashMap();
        config.put("http.agent.name", "this is only a test");

        bolt.prepare(config, TestUtil.getMockedTopologyContext(),
                new OutputCollector(output));

        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent()).thenReturn("source");
        when(tuple.getStringByField("url")).thenReturn(
                "http://localhost:" + port + "/");
        when(tuple.getValueByField("metadata")).thenReturn(null);
        bolt.execute(tuple);

        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
        }

        boolean acked = output.getAckedTuples().contains(tuple);
        boolean failed = output.getAckedTuples().contains(tuple);

        // should be acked or failed
        Assert.assertEquals(true, acked || failed);

        List<List<Object>> statusTuples = output
                .getEmitted(Constants.StatusStreamName);

        // we should get one tuple on the status stream
        // to notify that the URL has been fetched
        Assert.assertEquals(1, statusTuples.size());

        // and none on the default stream as there is nothing to parse and/or
        // index
        Assert.assertEquals(0, output.getEmitted(Utils.DEFAULT_STREAM_ID)
                .size());
    }

}
