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
package com.digitalpebble.stormcrawler.json;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.TestUtil;
import com.digitalpebble.stormcrawler.bolt.JSoupParserBolt;
import com.digitalpebble.stormcrawler.parse.ParsingTester;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JsoupFilterTest extends ParsingTester {

    @Before
    public void setupParserBolt() {
        bolt = new JSoupParserBolt();
        setupParserBolt(bolt);
    }

    protected void prepareParserBolt(String configFile, Map parserConfig) {
        parserConfig.put("jsoup.filters.config.file", configFile);
        bolt.prepare(
                parserConfig, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
    }

    @Test
    public void testLDJsonExtraction() throws IOException {

        prepareParserBolt("test.jsoupfilters.json");

        parse("http://www.digitalpebble.com", "digitalpebble.com.html");

        Assert.assertEquals(1, output.getEmitted().size());
        List<Object> parsedTuple = output.getEmitted().get(0);
        Metadata metadata = (Metadata) parsedTuple.get(2);
        Assert.assertNotNull(metadata);
        String[] scripts = metadata.getValues("streetAddress");
        Assert.assertNotNull(scripts);
    }

    @Test
    public void testLinkFilter() throws IOException {

        prepareParserBolt("test.jsoupfilters.json");

        parse("http://www.digitalpebble.com", "digitalpebble.com.html");
        List<List<Object>> status = output.getEmitted("status");
        Assert.assertEquals(16, status.size());
        List<Object> parsedTuple = status.get(0);
        parsedTuple.toArray();
    }
}
