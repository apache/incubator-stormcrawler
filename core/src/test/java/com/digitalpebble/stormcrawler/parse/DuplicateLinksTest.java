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

import com.digitalpebble.stormcrawler.Constants;
import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.TestUtil;
import com.digitalpebble.stormcrawler.bolt.JSoupParserBolt;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Checks that there aren't any duplicate links coming out of JSoupParser Bolt */
public class DuplicateLinksTest extends ParsingTester {

    @Before
    public void setupParserBolt() {
        bolt = new JSoupParserBolt();
        setupParserBolt(bolt);
    }

    @Test
    public void testSitemapSubdocuments() throws IOException {
        Map config = new HashMap();
        // generate a dummy config file
        config.put("urlfilters.config.file", "basicurlnormalizer.json");
        bolt.prepare(config, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        Metadata metadata = new Metadata();
        parse("http://www.digitalpebble.com/duplicates.html", "duplicateLinks.html", metadata);
        Assert.assertEquals(1, output.getEmitted(Constants.StatusStreamName).size());
    }
}
