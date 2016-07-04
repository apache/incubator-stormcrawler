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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import com.digitalpebble.stormcrawler.Constants;
import com.digitalpebble.stormcrawler.TestOutputCollector;
import com.digitalpebble.stormcrawler.TestUtil;

public abstract class AbstractFetcherBoltTest {

    BaseRichBolt bolt;

    @Test
    public void testDodgyURL() throws IOException {

        TestOutputCollector output = new TestOutputCollector();

        Map config = new HashMap();
        config.put("http.agent.name", "this is only a test");

        bolt.prepare(config, TestUtil.getMockedTopologyContext(),
                new OutputCollector(output));

        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent()).thenReturn("source");
        when(tuple.getStringByField("url")).thenReturn("ahahaha");
        when(tuple.getValueByField("metadata")).thenReturn(null);
        bolt.execute(tuple);

        boolean acked = output.getAckedTuples().contains(tuple);
        boolean failed = output.getAckedTuples().contains(tuple);

        // should be acked or failed
        Assert.assertEquals(true, acked || failed);

        List<List<Object>> statusTuples = output
                .getEmitted(Constants.StatusStreamName);

        // we should get one tuple on the status stream
        // to notify that the URL is an error
        Assert.assertEquals(1, statusTuples.size());
    }

}
