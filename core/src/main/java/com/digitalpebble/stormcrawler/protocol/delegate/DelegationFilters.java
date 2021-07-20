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
import com.fasterxml.jackson.databind.JsonNode;
import com.digitalpebble.stormcrawler.JSONResource;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class DelegationFilters implements JSONResource {
    protected String configFile;
    protected Config stormConf;

    protected Protocol robotsProtocol;
    protected Protocol defaultProtocol;
    protected ArrayList<JsonNode> protocolFilters;
    protected ArrayList<Protocol> protocolImplementations;

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(DelegationFilters.class);

    /**
     * Loads and configure the DelegationFilters based on the storm config.
     **/
    public static DelegationFilters fromConf(Config stormConf) {
        // load delegation configuration file
        String delegationConfigFile = ConfUtils.getString(stormConf, "protocol.delegate.file");

        // attempt to create a new delegation filter
        try {
            return new DelegationFilters(stormConf, delegationConfigFile);
        } catch (IOException e) {
            String message = "Exception caught while loading the DelegationFilters from " + delegationConfigFile;
            LOG.error(message);
            throw new RuntimeException(message, e);
        }
    }


    /**
     * loads the filters from a JSON configuration file
     *
     * @throws IOException
     */
    public DelegationFilters(Config stormConf, String configFile) throws IOException {
        // set instance variables from arguments
        this.configFile = configFile;
        this.stormConf = stormConf;

        // create empty lists for the protocols and filters
        this.protocolFilters = new ArrayList();
        this.protocolImplementations = new ArrayList();

        // attempt to load json
        try {
            loadJSONResources();
        } catch (Exception e) {
            throw new IOException("Unable to build JSON object from file", e);
        }
    }

    @Override
    public String getResourceFile() {
        return this.configFile;
    }

    @Override
    public void loadJSONResources(InputStream inputStream)
            throws JsonParseException, JsonMappingException, IOException {
        // create new object mapper to load json file with
        ObjectMapper mapper = new ObjectMapper();
        // load json file into mapper an process into a json node
        JsonNode confNode = mapper.readValue(inputStream, JsonNode.class);

        // ensure json in in the proper format
        if (!confNode.isArray())
            throw new RuntimeException("delegation filter json should be an array of objects");

        // iterate until we have loaded all of the protocols
        for (final JsonNode proto : confNode) {
            // retrieve protocol implementation for filter set
            String protocolImplementation = proto.get("proto").asText();
            try {
                // retrieve class for protocol implementation
                Class protocolClass = Class.forName(protocolImplementation);
                // confirm that the loaded class is a protocol implementation
                boolean interfaceOK = Protocol.class
                        .isAssignableFrom(protocolClass);
                // ensure that the class is an implementation of the Protocol interface
                if (!interfaceOK) {
                    throw new RuntimeException("Class " + protocolImplementation
                            + " does not implement Protocol");
                }
                // instantiate a new class for the passed protocol
                Protocol protocol = (Protocol) protocolClass.newInstance();
                // configure the class
                protocol.configure(stormConf);

                // check if protocol is the robots protocol
                if (proto.get("robots") != null && proto.get("robots").asBoolean())
                    // set robots protocol index
                    robotsProtocol = protocol;

                // skip protocols that had no value passed for filters
                if (proto.get("filters") == null) {
                    // move to next protocol if one exists
                    continue;
                }

                // conditionally handle default protocol
                if (proto.get("filters").isNull()) {
                    // set default protocol
                    defaultProtocol = protocol;
                    // move to next protocol if one exists
                    continue;
                }

                // store the class and its corresponding filters
                protocolImplementations.add(protocol);
                protocolFilters.add(proto.get("filters"));
            } catch (Exception e) {
                throw new RuntimeException("Failed to load protocol implementations form delegator configuration file", e);
            }
        }

        // ensure that a default protocol was passed
        if (defaultProtocol == null)
            throw new RuntimeException("no default protocol passed in delegation configuration file; pass a default protocol by setting `filters` to null");

        // if no robots protocol was explicitly specified then set the robots protocol to the default implementation
        if (robotsProtocol == null)
            robotsProtocol = defaultProtocol;
    }

    public Protocol getProtocol(String url, Metadata meta) {
        // iterate over protocol filters search for the first matching filter set
        for (int i = 0; i < protocolFilters.size(); i++) {
            // retrieve filter set to perform the filter operation
            final JsonNode filterSets = protocolFilters.get(i);

            // iterate over filter sets searching for a match
            for (final JsonNode filterSet : filterSets) {
                // retrieve filter fields from filter set
                String keyFilter = (filterSet.get("key") != null) ? filterSet.get("key").asText() : null;
                JsonNode metaFilter = filterSet.get("metadata");

                // conditionally filter for key
                if (StringUtils.isNotBlank(keyFilter)) {
                    // create object to hold loaded url
                    URL urlObject;
                    // load the url string into url object
                    try {
                        urlObject = new URL(url);
                    } catch (MalformedURLException e) {
                        throw new RuntimeException("failed to load target url into url object", e);
                    }

                    // skip protocol if host does not match the specified key
                    if (!keyFilter.equals(urlObject.getHost()))
                        continue;
                }

                // conditionally filter for metadata
                if (metaFilter != null) {
                    // create boolean to track whether we should skip this protocol
                    boolean skip = false;

                    // retrieve iterator from json node
                    Iterator<Map.Entry<String, JsonNode>> it = metaFilter.fields();

                    // iterate over kv pairs int meta filter checking if they match the metadata
                    while (it.hasNext()) {
                        // load next entry set from the iterator
                        Map.Entry<String, JsonNode> entry = it.next();

                        // load key and value from iterator
                        String k = entry.getKey();
                        JsonNode v = entry.getValue();

                        // skip protocol if the filter key does not exist in the metadata
                        if (StringUtils.isBlank(meta.getFirstValue(k))) {
                            // set skip variable to true
                            skip = true;
                            // break filter iteration
                            break;
                        }

                        // load meta field
                        String metaField = meta.getFirstValue(k);

                        // route filter to array ops or single value ops
                        if (v.isArray()) {
                            // create boolean to track if any of the values in the array match
                            boolean match = false;

                            // skip if meta field doesn't match any of the filter options
                            for (final JsonNode val : v) {
                                // load value to string and compare
                                if (val.asText().equals(metaField)) {
                                    // mark match as true
                                    match = true;
                                    // break iteration as we have already found a match
                                    break;
                                }
                            }

                            // skip protocol if the meta value did not match any of the filters in the array
                            if (!match) {
                                // set skip variable to true
                                skip = true;
                                // break filter iteration
                                break;
                            }
                        } else {
                            // load value to string and compare
                            if (!v.asText().equals(metaField)) {
                                // set skip variable to true
                                skip = true;
                                // break filter iteration
                                break;
                            }
                        }
                    }

                    // skip protocol if filters did not match
                    if (skip)
                        continue;
                }

                // debug log the filter match
                LOG.debug("delegation filter matched url - meta pair {} - {} with meta filter {}", url, meta, filterSet);

                // return protocol is all filters matched
                return protocolImplementations.get(i);
            }
        }

        // fallback on default protocol
        return defaultProtocol;
    }

    public void cleanup() {
        defaultProtocol.cleanup();
        for (Protocol proto : protocolImplementations) {
            proto.cleanup();
        }
    }

    public Protocol getDefaultProtocol() {
        return defaultProtocol;
    }

    public Protocol getRobotsProtocol() {
        return robotsProtocol;
    }
}