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

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import org.apache.storm.Config;

/** SingleProxyManager is a ProxyManager implementation for a single proxy endpoint */
public class SingleProxyManager implements ProxyManager {
    private SCProxy proxy;

    public SingleProxyManager() {}

    public void configure(Config conf) {
        // values for single proxy
        String proxyHost = ConfUtils.getString(conf, "http.proxy.host", null);
        String proxyType = ConfUtils.getString(conf, "http.proxy.type", "HTTP");
        int proxyPort = ConfUtils.getInt(conf, "http.proxy.port", 8080);
        String proxyUsername = ConfUtils.getString(conf, "http.proxy.user", null);
        String proxyPassword = ConfUtils.getString(conf, "http.proxy.pass", null);

        // assemble proxy connection string
        String proxyString = proxyType.toLowerCase() + "://";

        // conditionally append authentication info
        if (proxyUsername != null
                && !proxyUsername.isEmpty()
                && proxyPassword != null
                && !proxyPassword.isEmpty()) {
            proxyString += proxyUsername + ":" + proxyPassword + "@";
        }

        // complete proxy string and create proxy
        this.proxy = new SCProxy(proxyString + String.format("%s:%d", proxyHost, proxyPort));
    }

    @Override
    public SCProxy getProxy(Metadata metadata) {
        return proxy;
    }
}
