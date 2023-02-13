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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.storm.Config;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.parser.Parser;
import org.junit.Test;

public class TextExtractorTest {

    @Test
    public void testMainContent() throws IOException {
        Config conf = new Config();
        conf.put(TextExtractor.INCLUDE_PARAM_NAME, "DIV[id=\"maincontent\"]");

        TextExtractor extractor = new TextExtractor(conf);

        String content =
                "<html>the<div id='maincontent'>main<div>content</div></div>of the page</html>";

        Document jsoupDoc = Parser.htmlParser().parseInput(content, "http://stormcrawler.net");
        String text = extractor.text(jsoupDoc.body());

        assertEquals("main content", text);
    }

    @Test
    public void testExclusion() throws IOException {
        Config conf = new Config();
        conf.put(TextExtractor.EXCLUDE_PARAM_NAME, "STYLE");

        TextExtractor extractor = new TextExtractor(conf);

        String content = "<html>the<style>main</style>content of the page</html>";

        Document jsoupDoc = Parser.htmlParser().parseInput(content, "http://stormcrawler.net");
        String text = extractor.text(jsoupDoc.body());

        assertEquals("the content of the page", text);
    }

    @Test
    public void testExclusionCase() throws IOException {
        Config conf = new Config();
        conf.put(TextExtractor.EXCLUDE_PARAM_NAME, "style");

        TextExtractor extractor = new TextExtractor(conf);

        String content = "<html>the<STYLE>main</STYLE>content of the page</html>";

        Document jsoupDoc = Parser.htmlParser().parseInput(content, "http://stormcrawler.net");
        String text = extractor.text(jsoupDoc.body());

        assertEquals("the content of the page", text);
    }

    @Test
    public void testTrimContent() throws IOException {
        Config conf = new Config();
        List<String> listinc = new LinkedList<>();
        listinc.add("ARTICLE");
        conf.put(TextExtractor.INCLUDE_PARAM_NAME, listinc);

        List<String> listex = new LinkedList<>();
        listex.add("STYLE");
        listex.add("SCRIPT");

        conf.put(TextExtractor.EXCLUDE_PARAM_NAME, listex);

        // set a limit
        conf.put(TextExtractor.TEXT_MAX_TEXT_PARAM_NAME, 5000000);

        TextExtractor extractor = new TextExtractor(conf);

        String filename = "longtext.html";

        Document jsoupDoc =
                Jsoup.parse(
                        getClass().getClassLoader().getResourceAsStream(filename),
                        "windows-1252",
                        "http://ilovelongtext.com");

        String text = extractor.text(jsoupDoc.body());

        // one character gets added
        assertEquals(5000015, text.length());
    }
}
