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
import java.io.FileNotFoundException;
import org.apache.storm.Config;
import org.junit.Assert;
import org.junit.Test;

public class DelegationProtocolTest {

    private static final String OKHTTP =
            "com.digitalpebble.stormcrawler.protocol.okhttp.HttpProtocol";
    private static final String APACHE =
            "com.digitalpebble.stormcrawler.protocol.httpclient.HttpProtocol";

    @Test
    public void getProtocolTest() throws FileNotFoundException {

        Config conf = new Config();

        ConfUtils.loadConf("src/test/resources/delegator-conf.yaml", conf);

        conf.put("http.agent.name", "this.is.only.a.test");

        DelegatorProtocol superProto = new DelegatorProtocol();
        superProto.configure(conf);

        // try single filter
        // TODO use a protocol which doesnt require an actual connection when
        // configured
        Metadata meta = new Metadata();
        meta.setValue("js", "true");

        FilteredProtocol pf = superProto.getProtocolFor("https://digitalpebble.com", meta);

        Assert.assertEquals(pf.getProtocolInstance().getClass().getName(), OKHTTP);

        // no filter at all
        meta = new Metadata();
        pf = superProto.getProtocolFor("https://www.example.com/robots.txt", meta);

        Assert.assertEquals(pf.getProtocolInstance().getClass().getName(), OKHTTP);

        // should match the last instance
        // as the one above has more than one filter
        meta = new Metadata();
        meta.setValue("domain", "example.com");

        pf = superProto.getProtocolFor("https://example.com", meta);

        Assert.assertEquals(pf.getProtocolInstance().getClass().getName(), OKHTTP);

        // everything should match
        meta = new Metadata();
        meta.setValue("test", "true");
        meta.setValue("depth", "3");
        meta.setValue("domain", "example.com");

        pf = superProto.getProtocolFor("https://www.example-two.com", meta);

        Assert.assertEquals(pf.getProtocolInstance().getClass().getName(), APACHE);

        // should not match
        meta = new Metadata();
        meta.setValue("test", "false");
        meta.setValue("depth", "3");
        meta.setValue("domain", "example.com");

        pf = superProto.getProtocolFor("https://www.example-two.com", meta);

        Assert.assertEquals(pf.getProtocolInstance().getClass().getName(), OKHTTP);
    }
}
