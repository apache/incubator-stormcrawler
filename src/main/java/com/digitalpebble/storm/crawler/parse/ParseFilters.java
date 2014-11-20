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

package com.digitalpebble.storm.crawler.parse;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Wrapper for the ParseFilters defined in a JSON configuration **/
public class ParseFilters implements ParseFilter {

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(ParseFilters.class);

    private ParseFilter[] filters;

    public final static ParseFilter emptyParseFilter = new ParseFilters();

    private ParseFilters() {
        filters = new ParseFilter[0];
    }

    /**
     * loads the filters from a JSON configuration file
     *
     * @throws IOException
     * @throws JsonMappingException
     * @throws JsonParseException
     **/

    public ParseFilters(String configFile) throws IOException {
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

        configure(confNode);
    }

    @Override
    public void configure(JsonNode jsonNode) {
        // initialises the filters
        List<ParseFilter> filterLists = new ArrayList<ParseFilter>();

        // get the filters part
        String name = getClass().getCanonicalName();
        jsonNode = jsonNode.get(name);

        if (jsonNode == null) {
            LOG.info("No field " + name + " in JSON config. Skipping");
            filters = new ParseFilter[0];
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
            // check that it is available and implements the interface
            // ParseFilter
            try {
                Class<?> filterClass = Class.forName(className);
                boolean interfaceOK = ParseFilter.class
                        .isAssignableFrom(filterClass);
                if (!interfaceOK) {
                    LOG.error("Filter {} does not implement ParseFilter",
                            filterName);
                    continue;
                }
                ParseFilter filterInstance = (ParseFilter) filterClass
                        .newInstance();

                JsonNode paramNode = afilterNode.get("params");
                if (paramNode != null)
                    filterInstance.configure(paramNode);
                else
                    LOG.info("No field 'params' for filer {}", filterName);

                filterLists.add(filterInstance);
                LOG.info("Setup {}", filterName);
            } catch (Exception e) {
                LOG.error("Can't setup {}: {}", filterName, e);
                throw new RuntimeException("Can't setup " + filterName, e);
            }
        }

        filters = filterLists.toArray(new ParseFilter[filterLists.size()]);
    }

    @Override
    public boolean needsDOM() {
        for (ParseFilter filter : filters) {
            boolean needsDOM = filter.needsDOM();
            if (needsDOM)
                return true;
        }
        return false;
    }

    @Override
    public void filter(String URL, byte[] content, DocumentFragment doc,
            HashMap<String, String[]> metadata) {
        for (ParseFilter filter : filters) {
            filter.filter(URL, content, doc, metadata);
        }
    }

}
