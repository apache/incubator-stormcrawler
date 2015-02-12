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

package com.digitalpebble.storm.crawler.filtering;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.LoggerFactory;

import com.digitalpebble.storm.crawler.Metadata;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;

/**
 * Wrapper for the URLFilters defined in a JSON configuration
 */
public class URLFilters implements URLFilter {

    public static final URLFilters emptyURLFilters = new URLFilters();

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(URLFilters.class);

    private URLFilter[] filters;

    private URLFilters() {
        filters = new URLFilters[0];
    }

    /**
     * Loads the filters from a JSON configuration file
     *
     * @throws IOException
     */
    public URLFilters(Map stormConf, String configFile) throws IOException {
        // load the JSON configFile
        // build a JSON object out of it
        JsonNode confNode;
        try (InputStream confStream = getClass().getClassLoader()
                .getResourceAsStream(configFile)) {
            ObjectMapper mapper = new ObjectMapper();
            confNode = mapper.readValue(confStream, JsonNode.class);
        } catch (Exception e) {
            throw new IOException("Unable to build JSON object from file", e);
        }

        configure(stormConf, confNode);
    }

    @Override
    public String filter(URL sourceUrl, Metadata sourceMetadata,
            String urlToFilter) {
        String normalizedURL = urlToFilter;
        for (URLFilter filter : filters) {
            long start = System.currentTimeMillis();
            normalizedURL = filter.filter(sourceUrl, sourceMetadata,
                    normalizedURL);
            long end = System.currentTimeMillis();
            LOG.debug("URLFilter {} took {} msec", filter.getClass().getName(),
                    (end - start));
            if (normalizedURL == null)
                break;
        }
        return normalizedURL;
    }

    @Override
    public void configure(Map stormConf, JsonNode jsonNode) {
        // initialises the filters
        List<URLFilter> filterLists = new ArrayList<URLFilter>();

        // get the filters part
        String name = getClass().getCanonicalName();
        jsonNode = jsonNode.get(name);

        if (jsonNode == null) {
            LOG.info("No field {} in JSON config. Skipping", name);
            filters = new URLFilter[0];
            return;
        }

        // conf node contains a list of objects
        Iterator<JsonNode> filterIter = jsonNode.elements();
        while (filterIter.hasNext()) {
            JsonNode afilterNode = filterIter.next();
            String filterName = "<unnamed>";
            JsonNode nameNode = afilterNode.get("name");
            if (nameNode != null) {
                filterName = nameNode.textValue();
            }
            JsonNode classNode = afilterNode.get("class");
            if (classNode == null) {
                LOG.error("Filter {} doesn't specified a 'class' attribute",
                        filterName);
                continue;
            }
            String className = classNode.textValue().trim();
            filterName += '[' + className + ']';

            // check that it is available and implements the interface URLFilter
            try {
                Class<?> filterClass = Class.forName(className);
                boolean interfaceOK = URLFilter.class
                        .isAssignableFrom(filterClass);
                if (!interfaceOK) {
                    LOG.error("Class {} does not implement URLFilter",
                            className);
                    continue;
                }
                URLFilter filterInstance = (URLFilter) filterClass
                        .newInstance();

                JsonNode paramNode = afilterNode.get("params");
                if (paramNode != null) {
                    filterInstance.configure(stormConf, paramNode);
                } else {
                    filterInstance.configure(stormConf, NullNode.getInstance());
                }

                filterLists.add(filterInstance);
                LOG.info("Loaded instance of class {}", className);
            } catch (Exception e) {
                LOG.error("Can't setup {}: {}", filterName, e);
                continue;
            }
        }

        filters = filterLists.toArray(new URLFilter[filterLists.size()]);
    }
}
