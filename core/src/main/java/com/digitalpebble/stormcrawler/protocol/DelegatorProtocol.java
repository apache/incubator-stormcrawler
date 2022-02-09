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
import com.digitalpebble.stormcrawler.util.InitialisationUtil;
import crawlercommons.robots.BaseRobotRules;
import java.util.*;
import org.apache.storm.Config;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

/**
 * Protocol implementation that enables selection from a collection of sub-protocols using filters
 * based on each call's metadata
 *
 * <p>Is configured like this
 *
 * <pre>
 * protocol.delegator.config:
 * - className: "com.digitalpebble.stormcrawler.protocol.httpclient.HttpProtocol"
 *   filters:
 *     domain: "example.com"
 *     depth: "3"
 *     test: "true"
 * - className: "com.digitalpebble.stormcrawler.protocol.okhttp.HttpProtocol"
 *   filters:
 *     js: "true"
 * - className: "com.digitalpebble.stormcrawler.protocol.okhttp.HttpProtocol"
 * </pre>
 *
 * The last one in the list must not have filters as it is used as a default value. The protocols
 * are tried for matches in the order in which they are listed in the configuration. The first to
 * match gets used to fetch a URL.
 *
 * @since 2.2
 */
public class DelegatorProtocol implements Protocol {

    private static final String DELEGATOR_CONFIG_KEY = "protocol.delegator.config";

    protected static final org.slf4j.Logger LOG = LoggerFactory.getLogger(DelegatorProtocol.class);

    static class Filter {

        String key;
        String value;

        public Filter(String k, String v) {
            key = k;
            value = v;
        }
    }

    static class FilteredProtocol {

        final Protocol protoInstance;
        final List<Filter> filters = new ArrayList<>();

        @NotNull
        Protocol getProtocolInstance() {
            return protoInstance;
        }

        public FilteredProtocol(@NotNull String protocolImpl, @NotNull Config config) {
            this(protocolImpl, config, null);
        }

        public FilteredProtocol(
                @NotNull String protocolImpl,
                @NotNull Config config,
                @Nullable Map<String, String> filterImpls) {

            protoInstance =
                    InitialisationUtil.initializeFromQualifiedName(protocolImpl, Protocol.class);

            protoInstance.configure(config);

            // instantiate filters
            if (filterImpls != null) {
                filterImpls.forEach((k, v) -> filters.add(new Filter(k, v)));
            }

            // log filters found
            LOG.info("Loaded {} filters for {}", filters.size(), protocolImpl);
        }

        public ProtocolResponse getProtocolOutput(String url, Metadata metadata) throws Exception {
            return protoInstance.getProtocolOutput(url, metadata);
        }

        public BaseRobotRules getRobotRules(String url) {
            return protoInstance.getRobotRules(url);
        }

        public void cleanup() {
            protoInstance.cleanup();
        }

        boolean isMatch(final Metadata metadata) {
            // if this FP has no filters - it can handle anything
            if (filters.isEmpty()) return true;

            // check that all its filters are satisfied
            for (Filter f : filters) {
                if (f.value == null || f.value.equals("")) {
                    // just interested in the fact that the key exists
                    if (!metadata.containsKey(f.key)) {
                        LOG.trace("Key {} not found in metadata {}", f.key, metadata);
                        return false;
                    }
                } else {
                    // interested in the value associated with the key
                    if (!metadata.containsKeyWithValue(f.key, f.value)) {
                        LOG.trace(
                                "Key {} not found with value {} in metadata {}",
                                f.key,
                                f.value,
                                metadata);
                        return false;
                    }
                }
            }

            return true;
        }
    }

    private final LinkedList<FilteredProtocol> protocols = new LinkedList<>();

    @Override
    public void configure(@NotNull Config conf) {
        Object obj = conf.get(DELEGATOR_CONFIG_KEY);

        if (obj == null)
            throw new RuntimeException("DelegatorProtocol declared but no config set for it");

        // should contain a list of maps
        // each map having a className and optionally a number of filters
        if (obj instanceof Iterable) {
            //noinspection unchecked
            for (Map<String, Object> subConf : (Iterable<? extends Map<String, Object>>) obj) {
                String className = (String) subConf.get("className");
                Object filters = subConf.get("filters");
                FilteredProtocol protocol;
                if (filters == null) {
                    protocol = new FilteredProtocol(className, conf);
                } else {
                    //noinspection unchecked
                    protocol = new FilteredProtocol(className, conf, (Map<String, String>) filters);
                }
                protocols.add(protocol);
            }
        } else { // single value?
            throw new RuntimeException(
                    "DelegatorProtocol declared but single object found in config " + obj);
        }

        if (protocols.isEmpty()) {
            throw new RuntimeException("No sub protocols for delegation protocol defined.");
        }

        // check that the last protocol has no filter
        if (!protocols.peekLast().filters.isEmpty()) {
            throw new RuntimeException(
                    "The last sub protocol has filters but must not as it acts as the default");
        }
    }

    final FilteredProtocol getProtocolFor(String url, Metadata metadata) {

        for (FilteredProtocol p : protocols) {
            if (p.isMatch(metadata)) {
                return p;
            }
        }

        return null;
    }

    @Override
    public @NotNull BaseRobotRules getRobotRules(@NotNull String url) {

        FilteredProtocol proto = getProtocolFor(url, Metadata.empty);
        if (proto == null) {
            throw new RuntimeException("No sub protocol eligible to retrieve robots");
        }
        return proto.getRobotRules(url);
    }

    @Override
    public @NotNull ProtocolResponse getProtocolOutput(
            @NotNull String url, @NotNull Metadata metadata) throws Exception {

        // go through the filtered protocols to find which one to use
        FilteredProtocol proto = getProtocolFor(url, metadata);
        if (proto == null) {
            throw new RuntimeException(
                    "No sub protocol eligible to retrieve " + url + "given " + metadata);
        }
        // execute and return protocol with url-meta combo
        return proto.getProtocolOutput(url, metadata);
    }

    @Override
    public void cleanup() {
        for (FilteredProtocol p : protocols) p.cleanup();
    }
}
