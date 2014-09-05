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

package com.digitalpebble.storm.crawler.protocol;

import java.net.URL;
import java.util.WeakHashMap;

import com.digitalpebble.storm.crawler.protocol.http.HttpProtocol;

import backtype.storm.Config;

public class ProtocolFactory {

    private final Config config;

    private final WeakHashMap<String, Protocol> cache = new WeakHashMap<String, Protocol>();

    public ProtocolFactory(Config conf) {
        config = conf;
    }

    /** Returns an instance of the protocol to use for a given URL **/
    public synchronized Protocol getProtocol(URL url) {
        // get the protocol
        String protocol = url.getProtocol();
        Protocol pp = cache.get(protocol);
        if (pp != null)
            return pp;

        // yuk! hardcoded for now
        pp = new HttpProtocol();
        pp.configure(config);
        cache.put(protocol, pp);
        return pp;
    }

}
