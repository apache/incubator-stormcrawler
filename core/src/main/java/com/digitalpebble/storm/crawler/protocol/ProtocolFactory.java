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
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;

import backtype.storm.Config;

import com.digitalpebble.storm.crawler.util.ConfUtils;

public class ProtocolFactory {

    private final Config config;

    private final HashMap<String, Protocol> cache = new HashMap<String, Protocol>();

    public ProtocolFactory(Config conf) {
        config = conf;

        // load the list of protocols
        String[] protocols = ConfUtils.getString(conf, "protocols",
                "http,https").split(" *, *");

        // load the class names for each protocol
        // e.g. http.protocol.implementation
        for (String protocol : protocols) {
            String paramName = protocol + ".protocol.implementation";
            String protocolimplementation = ConfUtils
                    .getString(conf, paramName);
            if (StringUtils.isBlank(protocolimplementation)) {
                // set the default values
                if (protocol.equalsIgnoreCase("http")) {
                    protocolimplementation = "com.digitalpebble.storm.crawler.protocol.httpclient.HttpProtocol";
                } else if (protocol.equalsIgnoreCase("https")) {
                    protocolimplementation = "com.digitalpebble.storm.crawler.protocol.httpclient.HttpProtocol";
                } else
                    throw new RuntimeException(paramName
                            + "should not have an empty value");
            }
            // we have a value -> is it correct?
            Class protocolClass = null;
            try {
                protocolClass = Class.forName(protocolimplementation);
                boolean interfaceOK = Protocol.class
                        .isAssignableFrom(protocolClass);
                if (!interfaceOK) {
                    throw new RuntimeException("Class "
                            + protocolimplementation
                            + " does not implement Protocol");
                }
                Protocol protoInstance = (Protocol) protocolClass.newInstance();
                protoInstance.configure(config);
                cache.put(protocol, protoInstance);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Can't load class "
                        + protocolimplementation);
            } catch (InstantiationException e) {
                throw new RuntimeException("Can't instanciate class "
                        + protocolimplementation);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("IllegalAccessException for class "
                        + protocolimplementation);
            }
        }

    }

    /** Returns an instance of the protocol to use for a given URL */
    public synchronized Protocol getProtocol(URL url) {
        // get the protocol
        String protocol = url.getProtocol();
        return cache.get(protocol);
    }
}
