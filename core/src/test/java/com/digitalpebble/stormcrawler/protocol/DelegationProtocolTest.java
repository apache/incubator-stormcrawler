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
package com.digitalpebble.stormcrawler.protocol;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.protocol.DelegatorProtocol.FilteredProtocol;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import org.apache.storm.Config;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DelegationProtocolTest {

    private static final String OKHTTP =
            "com.digitalpebble.stormcrawler.protocol.okhttp.HttpProtocol";
    private static final String APACHE =
            "com.digitalpebble.stormcrawler.protocol.httpclient.HttpProtocol";

    private static final Config conf = new Config();

    @BeforeClass
    public static void beforeClass() throws Exception {
        ConfUtils.loadConf("src/test/resources/delegator-conf.yaml", conf);
        conf.put("http.agent.name", "this.is.only.a.test");
    }

    private static DelegatorProtocol getInstance() {
        DelegatorProtocol superProto = new DelegatorProtocol();
        superProto.configure(conf);
        return superProto;
    }

    @Test
    public void single_filter() {
        DelegatorProtocol superProto = getInstance();

        // try single filter
        // TODO use a protocol which doesnt require an actual connection when
        // configured
        Metadata meta = new Metadata();
        meta.setValue("js", "true");

        FilteredProtocol pf = superProto.getProtocolFor("https://digitalpebble.com", meta);

        Assert.assertEquals(pf.getProtocolInstance().getClass().getName(), OKHTTP);
    }

    @Test
    public void no_filter() {
        DelegatorProtocol superProto = getInstance();
        Metadata meta = new Metadata();
        FilteredProtocol pf = superProto.getProtocolFor("https://www.example.com/robots.txt", meta);
        Assert.assertEquals(pf.getProtocolInstance().getClass().getName(), OKHTTP);
    }

    @Test
    public void sould_match_last_instance() {
        DelegatorProtocol superProto = getInstance();

        // should match the last instance
        // as the one above has more than one filter
        Metadata meta = new Metadata();
        meta.setValue("domain", "example.com");

        FilteredProtocol pf = superProto.getProtocolFor("https://example.com", meta);

        Assert.assertEquals(pf.getProtocolInstance().getClass().getName(), OKHTTP);
    }

    @Test
    public void everything_should_match() {
        DelegatorProtocol superProto = getInstance();

        // everything should match
        Metadata meta = new Metadata();
        meta.setValue("test", "true");
        meta.setValue("depth", "3");
        meta.setValue("domain", "example.com");

        FilteredProtocol pf = superProto.getProtocolFor("https://www.example-two.com", meta);

        Assert.assertEquals(pf.getProtocolInstance().getClass().getName(), APACHE);
    }

    @Test
    public void does_not_match() {
        DelegatorProtocol superProto = getInstance();

        // should not match
        Metadata meta = new Metadata();
        meta.setValue("test", "false");
        meta.setValue("depth", "3");
        meta.setValue("domain", "example.com");

        FilteredProtocol pf = superProto.getProtocolFor("https://www.example-two.com", meta);

        Assert.assertEquals(pf.getProtocolInstance().getClass().getName(), OKHTTP);
    }
}
