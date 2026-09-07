/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.stormcrawler.tika;

import java.util.HashMap;
import java.util.Map;
import org.apache.stormcrawler.TestUtil;
import org.apache.tika.config.OutputLimits;
import org.apache.tika.parser.CompositeParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ocr.TesseractOCRParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Verifies what the bolt actually loads from the Tika configuration. */
class TikaConfigTest {

    @Test
    void testTesseractOCRExcludedByDefaultConfig() {
        ParserBolt bolt = new ParserBolt();
        bolt.prepare(new HashMap<>(), TestUtil.getMockedTopologyContext(), null);
        // the bundled configuration excludes the OCR parser: prove it rather
        // than assume the configuration was picked up
        Parser parser = bolt.getTika().getParser();
        Assertions.assertTrue(
                parser instanceof CompositeParser, "the loaded parser should be a CompositeParser");
        for (Parser p : ((CompositeParser) parser).getAllComponentParsers()) {
            Assertions.assertFalse(
                    p instanceof TesseractOCRParser,
                    "TesseractOCRParser should be excluded by the default configuration");
        }
    }

    @Test
    void testParseContextSeededFromConfig() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("parser.tika.config.file", "test-tika-config.json");
        ParserBolt bolt = new ParserBolt();
        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), null);

        // the "parse-context" section of the configuration must reach the
        // context used for each parse
        ParseContext parseContext = bolt.createParseContext();
        Assertions.assertEquals(
                100000,
                OutputLimits.get(parseContext).getWriteLimit(),
                "writeLimit from the parse-context section of the configuration");
        // no limits set by default
        Assertions.assertEquals(
                OutputLimits.UNLIMITED, OutputLimits.get(new ParseContext()).getWriteLimit());
    }

    @Test
    void testBrokenConfigFailsFast() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("parser.tika.config.file", "broken-tika-config.json");
        ParserBolt bolt = new ParserBolt();
        // an invalid configuration must abort the topology instead of
        // silently degrading to the default Tika configuration
        Assertions.assertThrows(
                IllegalStateException.class,
                () -> bolt.prepare(conf, TestUtil.getMockedTopologyContext(), null));
    }

    @Test
    void testMissingConfigFailsFast() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("parser.tika.config.file", "does-not-exist.json");
        ParserBolt bolt = new ParserBolt();
        Assertions.assertThrows(
                IllegalStateException.class,
                () -> bolt.prepare(conf, TestUtil.getMockedTopologyContext(), null));
    }
}
