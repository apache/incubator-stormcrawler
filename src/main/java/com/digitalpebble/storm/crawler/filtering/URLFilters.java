/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.LoggerFactory;

/** Wrapper for the URLFilters defined in a JSON configuration **/
public class URLFilters implements URLFilter {

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(URLFilters.class);

    private URLFilter[] filters;

    /**
     * loads the filters from a JSON configuration file
     * 
     * @throws IOException
     * @throws JsonMappingException
     * @throws JsonParseException
     **/

    public URLFilters(String configFile) throws IOException {
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
    public String filter(String URL) {
        for (URLFilter filter : filters) {
            String newURL = filter.filter(URL);
            if (newURL == null)
                return null;
            URL = newURL;
        }
        return URL;
    }

    @Override
    public void configure(JsonNode jsonNode) {
        // initialises the filters
        List<URLFilter> filterLists = new ArrayList<URLFilter>();

        // get the filters part
        String name = getClass().getCanonicalName();
        jsonNode = jsonNode.get(name);

        if (jsonNode == null) {
            LOG.info("No field " + name + " in JSON config. Skipping");
            filters = new URLFilter[0];
            return;
        }

        // conf node contains a list of objects
        Iterator<JsonNode> filterIter = jsonNode.getElements();
        while (filterIter.hasNext()) {
            JsonNode afilterNode = filterIter.next();
            JsonNode classNode = afilterNode.get("class");
            if (classNode == null)
                continue;
            String className = classNode.getTextValue().trim();
            // check that it is available and implements the interface URLFilter
            try {
                Class<?> filterClass = Class.forName(className);
                boolean interfaceOK = URLFilter.class
                        .isAssignableFrom(filterClass);
                if (!interfaceOK) {
                    LOG.error("Class " + className
                            + " does not implement URLFilter");
                    continue;
                }
                URLFilter filterInstance = (URLFilter) filterClass
                        .newInstance();

                JsonNode paramNode = afilterNode.get("params");
                if (paramNode != null)
                    filterInstance.configure(paramNode);
                else
                    LOG.info("No field 'params' for instance of class "
                            + className);

                filterLists.add(filterInstance);
                LOG.info("Loaded instance of class " + className);
            } catch (Exception e) {
                LOG.error("Can't load or instanciate class : " + className);
                continue;
            }
        }

        filters = filterLists.toArray(new URLFilter[filterLists.size()]);
    }

    public static void main(String args[]) throws IOException {
        URLFilters filters = new URLFilters(args[0]);
    }

}
