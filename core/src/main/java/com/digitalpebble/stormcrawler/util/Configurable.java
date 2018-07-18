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

package com.digitalpebble.stormcrawler.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.parse.ParseFilters;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;

public interface Configurable {

    static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ParseFilters.class);

    /**
     * Called when this filter is being initialized
     * 
     * @param stormConf
     *            The Storm configuration used for the ParserBolt
     * @param filterParams
     *            the filter specific configuration. Never null
     */
    public default void configure(Map stormConf, JsonNode filterParams) {
    }

    /**
     * Used by classes URLFilters and ParseFilters classes to load the
     * configuration of filters from JSON
     **/
    @SuppressWarnings("rawtypes")
    public static <T extends Configurable> List<T> configure(Map stormConf, JsonNode filtersConf, Class<T> filterClass,
            String callingClass) {
        // initialises the filters
        List<T> filterLists = new ArrayList<>();

        // get the filters part
        filtersConf = filtersConf.get(callingClass);

        if (filtersConf == null) {
            LOG.info("No field {} in JSON config. Skipping", callingClass);
            return filterLists;
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
                LOG.error("Filter {} doesn't specified a 'class' attribute", filterName);
                continue;
            }
            String className = classNode.textValue().trim();
            filterName += '[' + className + ']';
            // check that it is available and implements the interface
            // ParseFilter
            try {
                Class<?> filterImplClass = Class.forName(className);
                boolean subClassOK = filterClass.isAssignableFrom(filterImplClass);
                if (!subClassOK) {
                    LOG.error("Filter {} does not extend {}", filterName, filterClass.getName());
                    continue;
                }
                T filterInstance = (T) filterImplClass.newInstance();

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

        return filterLists;
    }

}
