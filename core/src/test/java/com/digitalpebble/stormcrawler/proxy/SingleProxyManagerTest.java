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
package com.digitalpebble.stormcrawler.proxy;

import org.apache.storm.Config;
import org.junit.Assert;
import org.junit.Test;

public class SingleProxyManagerTest {
    @Test
    public void testSimpleProxyManager() throws RuntimeException {
        Config config = new Config();
        config.put("http.proxy.host", "example.com");
        config.put("http.proxy.type", "HTTP");
        config.put("http.proxy.port", 8080);
        config.put("http.proxy.user", "user1");
        config.put("http.proxy.pass", "pass1");

        SingleProxyManager pm = new SingleProxyManager();
        pm.configure(config);

        SCProxy proxy = pm.getProxy(null);

        Assert.assertEquals(proxy.getProtocol(), "http");
        Assert.assertEquals(proxy.getAddress(), "example.com");
        Assert.assertEquals(proxy.getPort(), "8080");
        Assert.assertEquals(proxy.getUsername(), "user1");
        Assert.assertEquals(proxy.getPassword(), "pass1");

        Assert.assertEquals(proxy.toString(), "http://user1:pass1@example.com:8080");
    }
}
