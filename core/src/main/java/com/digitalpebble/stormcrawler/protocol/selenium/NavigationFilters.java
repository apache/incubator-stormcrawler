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

package com.digitalpebble.stormcrawler.protocol.selenium;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;

/**
 * Wrapper for the NavigationFilter defined in a JSON configuration
 */
public class NavigationFilters extends NavigationFilter {

    public static final NavigationFilters emptyNavigationFilters = new NavigationFilters();

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(NavigationFilters.class);

    private NavigationFilter[] filters;

    private NavigationFilters() {
        filters = new NavigationFilter[0];
    }

    public ProtocolResponse filter(RemoteWebDriver driver, Metadata metadata) {
        for (NavigationFilter filter : filters) {
            ProtocolResponse response = filter.filter(driver, metadata);
            if (response != null)
                return response;
        }
        return null;
    }

    /**
     * Loads and configure the NavigationFilters based on the storm config if
     * there is one otherwise returns an emptyNavigationFilters.
     **/
    @SuppressWarnings("rawtypes")
    public static NavigationFilters fromConf(Map stormConf) {
        String configfile = ConfUtils.getString(stormConf,
                "navigationfilters.config.file");
        if (StringUtils.isNotBlank(configfile)) {
            try {
                return new NavigationFilters(stormConf, configfile);
            } catch (IOException e) {
                String message = "Exception caught while loading the NavigationFilters from "
                        + configfile;
                LOG.error(message);
                throw new RuntimeException(message, e);
            }
        }

        return NavigationFilters.emptyNavigationFilters;
    }

    /**
     * loads the filters from a JSON configuration file
     * 
     * @throws IOException
     */
    @SuppressWarnings("rawtypes")
    public NavigationFilters(Map stormConf, String configFile)
            throws IOException {
        // load the JSON configFile
        // build a JSON object out of it
        JsonNode confNode = null;
        InputStream confStream = null;
        try {
            confStream = getClass().getClassLoader().getResourceAsStream(
                    configFile);

            ObjectMapper mapper = new ObjectMapper();
            confNode = mapper.readValue(confStream, JsonNode.class);
        } catch (Exception e) {
            throw new IOException("Unable to build JSON object from file", e);
        } finally {
            if (confStream != null) {
                confStream.close();
            }
        }

        configure(stormConf, confNode);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void configure(Map stormConf, JsonNode filtersConf) {
        // initialises the filters
        List<NavigationFilter> filterLists = new ArrayList<>();

        // get the filters part
        String name = getClass().getCanonicalName();
        filtersConf = filtersConf.get(name);

        if (filtersConf == null) {
            LOG.info("No field {} in JSON config. Skipping", name);
            filters = new NavigationFilter[0];
            return;
        }

        // conf node contains a list of objects
        Iterator<JsonNode> filterIter = filtersConf.elements();
        while (filterIter.hasNext()) {
            JsonNode afilterConf = filterIter.next();
            String filterName = "<unnamed>";
            JsonNode nameNode = afilterConf.get("name");
            if (nameNode != null) {
                filterName = nameNode.textValue();
            }
            JsonNode classNode = afilterConf.get("class");
            if (classNode == null) {
                LOG.error("Filter {} doesn't specified a 'class' attribute",
                        filterName);
                continue;
            }
            String className = classNode.textValue().trim();
            filterName += '[' + className + ']';
            // check that it is available and implements the interface
            // NavigationFilter
            try {
                Class<?> filterClass = Class.forName(className);
                boolean subClassOK = NavigationFilter.class
                        .isAssignableFrom(filterClass);
                if (!subClassOK) {
                    LOG.error("Filter {} does not extend NavigationFilter",
                            filterName);
                    continue;
                }
                NavigationFilter filterInstance = (NavigationFilter) filterClass
                        .newInstance();

                JsonNode paramNode = afilterConf.get("params");
                if (paramNode != null) {
                    filterInstance.configure(stormConf, paramNode);
                } else {
                    // Pass in a nullNode if missing
                    filterInstance.configure(stormConf, NullNode.getInstance());
                }

                filterLists.add(filterInstance);
                LOG.info("Setup {}", filterName);
            } catch (Exception e) {
                LOG.error("Can't setup {}: {}", filterName, e);
                throw new RuntimeException("Can't setup " + filterName, e);
            }
        }

        filters = filterLists.toArray(new NavigationFilter[filterLists.size()]);
    }

}
