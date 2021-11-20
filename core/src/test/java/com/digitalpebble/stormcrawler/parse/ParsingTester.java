/**
 * Licensed to DigitalPebble Ltd under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.digitalpebble.stormcrawler.parse;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.TestOutputCollector;
import com.digitalpebble.stormcrawler.TestUtil;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.junit.After;

public class ParsingTester {
    protected BaseRichBolt bolt;
    protected TestOutputCollector output;

    protected void setupParserBolt(BaseRichBolt bolt) {
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

    protected void prepareParserBolt(String configFile) {
        prepareParserBolt(configFile, new HashMap());
    }

    protected void prepareParserBolt(String configFile, Map parserConfig) {
        parserConfig.put("parsefilters.config.file", configFile);
        bolt.prepare(
                parserConfig, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
    }

    protected void parse(String url, byte[] content, Metadata metadata) throws IOException {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getBinaryByField("content")).thenReturn(content);
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        bolt.execute(tuple);
    }

    protected void parse(String url, String filename) throws IOException {
        parse(url, filename, new Metadata());
    }

    protected void parse(String url, String filename, Metadata metadata) throws IOException {
        byte[] content = readContent(filename);
        Tuple tuple = mock(Tuple.class);
        when(tuple.getBinaryByField("content")).thenReturn(content);
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        bolt.execute(tuple);
    }

    protected byte[] readContent(String filename) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copy(getClass().getClassLoader().getResourceAsStream(filename), baos);
        return baos.toByteArray();
    }
}
