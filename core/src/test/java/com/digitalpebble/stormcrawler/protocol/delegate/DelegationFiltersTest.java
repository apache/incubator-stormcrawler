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

package com.digitalpebble.stormcrawler.protocol.delegate;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.protocol.Protocol;
import com.digitalpebble.stormcrawler.protocol.okhttp.HttpProtocol;
import com.digitalpebble.stormcrawler.protocol.playwright.PlaywrightProtocol;
import org.apache.storm.Config;
import org.junit.Test;
import org.junit.Assert;

public class DelegationFiltersTest {
    @Test
    public void fromConfTest() {
        Config conf = new Config();
        conf.put("protocol.delegate.file", "protocolDelegator.json");
        conf.put("http.agent.name", "test");

        DelegationFilters.fromConf(conf);
    }

    @Test
    public void getProtocolTest() {
        Config conf = new Config();
        conf.put("protocol.delegate.file", "protocolDelegator.json");
        conf.put("http.agent.name", "test");

        DelegationFilters delegationFilters = DelegationFilters.fromConf(conf);

        Metadata meta = new Metadata();
        meta.setValue("js", "true");

        Protocol proto = delegationFilters.getProtocol("https://digitalpebble.com", meta);

        Assert.assertEquals(proto.getClass().getName(), PlaywrightProtocol.class.getName());

        meta = new Metadata();

        proto = delegationFilters.getProtocol("https://www.example.com", meta);

        Assert.assertEquals(proto.getClass().getName(), PlaywrightProtocol.class.getName());

        meta = new Metadata();
        meta.setValue("domain", "example.com");

        proto = delegationFilters.getProtocol("https://example.com", meta);

        Assert.assertEquals(proto.getClass().getName(), PlaywrightProtocol.class.getName());

        meta = new Metadata();
        meta.setValue("test", "true");
        meta.setValue("depth", "3");
        meta.setValue("domain", "example-two.com");

        proto = delegationFilters.getProtocol("https://www.example-two.com", meta);

        Assert.assertEquals(proto.getClass().getName(), PlaywrightProtocol.class.getName());

        meta = new Metadata();
        meta.setValue("test", "false");
        meta.setValue("depth", "3");
        meta.setValue("domain", "example-two.com");

        proto = delegationFilters.getProtocol("https://www.example-two.com", meta);

        Assert.assertEquals(proto.getClass().getName(), HttpProtocol.class.getName());
    }

    @Test
    public void getDefaultProtocolTest() {
        Config conf = new Config();
        conf.put("protocol.delegate.file", "protocolDelegator.json");
        conf.put("http.agent.name", "test");

        DelegationFilters delegationFilters = DelegationFilters.fromConf(conf);

        Protocol proto = delegationFilters.getDefaultProtocol();

        Assert.assertEquals(proto.getClass().getName(), HttpProtocol.class.getName());
    }

    @Test
    public void getRobotsProtocolTest() {
        Config conf = new Config();
        conf.put("protocol.delegate.file", "protocolDelegator.json");
        conf.put("http.agent.name", "test");

        DelegationFilters delegationFilters = DelegationFilters.fromConf(conf);

        Protocol proto = delegationFilters.getRobotsProtocol();

        Assert.assertEquals(proto.getClass().getName(), com.digitalpebble.stormcrawler.protocol.httpclient.HttpProtocol.class.getName());
    }
}
