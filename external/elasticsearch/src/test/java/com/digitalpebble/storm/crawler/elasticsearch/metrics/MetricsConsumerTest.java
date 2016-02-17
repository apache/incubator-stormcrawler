/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.digitalpebble.storm.crawler.elasticsearch.metrics;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class MetricsConsumerTest {

    @Test
    public void should_not_skip_anything_if_no_whitelist() throws IOException {
        MetricsConsumer mc = new MetricsConsumer();
        assertFalse(mc.shouldSkip("metric"));

        List<String> whitelist = new ArrayList<>();
        mc.setWhitelist(whitelist);
        assertFalse(mc.shouldSkip("metric"));
    }

    @Test
    public void should_only_skip_items_not_in_whitelist() throws IOException {
        MetricsConsumer mc = new MetricsConsumer();
        List<String> whitelist = new ArrayList<>();
        whitelist.add("metric1");
        mc.setWhitelist(whitelist);

        assertFalse(mc.shouldSkip("metric1"));
        assertTrue(mc.shouldSkip("metric2"));
    }

    @Test
    public void should_always_skip_blacklist() throws IOException {
        MetricsConsumer mc = new MetricsConsumer();
        List<String> blacklist = new ArrayList<>();
        blacklist.add("metric1");
        mc.setBlacklist(blacklist);

        assertTrue(mc.shouldSkip("metric1"));
        assertFalse(mc.shouldSkip("metric2"));
    }

    @Test
    public void blacklist_overwrites_whitelist() throws IOException {
        MetricsConsumer mc = new MetricsConsumer();
        List<String> blacklist = new ArrayList<>();
        blacklist.add("metric1");
        mc.setBlacklist(blacklist);

        List<String> whitelist = new ArrayList<>();
        whitelist.add("metric1");
        whitelist.add("metric2");
        mc.setWhitelist(whitelist);

        assertTrue(mc.shouldSkip("metric1"));
        assertFalse(mc.shouldSkip("metric2"));
        assertTrue(mc.shouldSkip("metric3"));
    }

    @Test
    public void allow_partial_matching_whitelist() throws IOException {
        MetricsConsumer mc = new MetricsConsumer();
        List<String> whitelist = new ArrayList<>();
        whitelist.add("parent1.metric1");
        whitelist.add("parent2");
        mc.setWhitelist(whitelist);

        assertFalse(mc.shouldSkip("parent1.metric1"));
        assertTrue(mc.shouldSkip("parent1.metric2"));
        assertFalse(mc.shouldSkip("parent2"));
        assertFalse(mc.shouldSkip("parent2.metric2"));
    }

    @Test
    public void allow_partial_matching_blacklist() throws IOException {
        MetricsConsumer mc = new MetricsConsumer();
        List<String> blacklist = new ArrayList<>();
        blacklist.add("parent1.metric1");
        blacklist.add("parent2");
        mc.setBlacklist(blacklist);

        assertTrue(mc.shouldSkip("parent1.metric1"));
        assertFalse(mc.shouldSkip("parent1.metric2"));
        assertTrue(mc.shouldSkip("parent2"));
        assertTrue(mc.shouldSkip("parent2.metric2"));
    }

    @Test
    public void partial_blacklist_overwrites_partial_whitelist()
            throws IOException {
        MetricsConsumer mc = new MetricsConsumer();
        List<String> blacklist = new ArrayList<>();
        blacklist.add("metric1");
        mc.setBlacklist(blacklist);

        List<String> whitelist = new ArrayList<>();
        whitelist.add("metric1");
        whitelist.add("metric2");
        mc.setWhitelist(whitelist);

        assertTrue(mc.shouldSkip("metric1.child"));
        assertFalse(mc.shouldSkip("metric2.child"));
        assertTrue(mc.shouldSkip("metric3.child"));
    }
}
