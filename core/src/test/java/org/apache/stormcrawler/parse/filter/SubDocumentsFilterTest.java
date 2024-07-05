/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.stormcrawler.parse.filter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.bolt.JSoupParserBolt;
import org.apache.stormcrawler.parse.ParsingTester;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SubDocumentsFilterTest extends ParsingTester {

    @BeforeEach
    void setupParserBolt() {
        bolt = new JSoupParserBolt();
        setupParserBolt(bolt);
    }

    @Test
    void testSitemapSubdocuments() throws IOException {
        Map config = new HashMap();
        config.put("detect.mimetype", false);
        prepareParserBolt("test.subdocfilter.json", config);
        Metadata metadata = new Metadata();
        parse("http://www.digitalpebble.com/sitemap.xml", "digitalpebble.sitemap.xml", metadata);
        Assertions.assertEquals(6, output.getEmitted().size());
    }
}
