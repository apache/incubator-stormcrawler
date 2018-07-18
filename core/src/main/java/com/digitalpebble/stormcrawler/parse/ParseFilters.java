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

package com.digitalpebble.stormcrawler.parse;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

import com.digitalpebble.stormcrawler.JSONResource;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.Configurable;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Wrapper for the ParseFilters defined in a JSON configuration
 */
public class ParseFilters extends ParseFilter implements JSONResource {

    public static final ParseFilters emptyParseFilter = new ParseFilters();

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(ParseFilters.class);

    private ParseFilter[] filters;

    private ParseFilters() {
        filters = new ParseFilter[0];
    }

    private String configFile = "parsefilters.config.file";

    private Map stormConf;

    /**
     * Loads and configure the ParseFilters based on the storm config if there
     * is one otherwise returns an emptyParseFilter.
     **/
    @SuppressWarnings("rawtypes")
    public static ParseFilters fromConf(Map stormConf) {
        String parseconfigfile = ConfUtils.getString(stormConf,
                "parsefilters.config.file");
        if (StringUtils.isNotBlank(parseconfigfile)) {
            try {
                return new ParseFilters(stormConf, parseconfigfile);
            } catch (IOException e) {
                String message = "Exception caught while loading the ParseFilters from "
                        + parseconfigfile;
                LOG.error(message);
                throw new RuntimeException(message, e);
            }
        }

        return ParseFilters.emptyParseFilter;
    }

    /**
     * loads the filters from a JSON configuration file
     * 
     * @throws IOException
     */
    @SuppressWarnings("rawtypes")
    public ParseFilters(Map stormConf, String configFile) throws IOException {
        this.configFile = configFile;
        this.stormConf = stormConf;
        try {
            loadJSONResources();
        } catch (Exception e) {
            throw new IOException("Unable to build JSON object from file", e);
        }
    }

    @Override
    public void loadJSONResources(InputStream inputStream)
            throws JsonParseException, JsonMappingException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode confNode = mapper.readValue(inputStream, JsonNode.class);
        configure(stormConf, confNode);
    }

    @Override
    public String getResourceFile() {
        return this.configFile;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void configure(Map stormConf, JsonNode filtersConf) {
        List<ParseFilter> list = Configurable.configure(stormConf, filtersConf,
                ParseFilter.class, this.getClass().getName());
        filters = list.toArray(new ParseFilter[list.size()]);
    }

    @Override
    public boolean needsDOM() {
        for (ParseFilter filter : filters) {
            boolean needsDOM = filter.needsDOM();
            if (needsDOM) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void filter(String URL, byte[] content, DocumentFragment doc,
            ParseResult parse) {

        for (ParseFilter filter : filters) {
            long start = System.currentTimeMillis();
            if (doc == null && filter.needsDOM()) {
                LOG.info(
                        "ParseFilter {} needs DOM but has none to work on - skip : {}",
                        filter.getClass().getName(), URL);
                continue;
            }
            filter.filter(URL, content, doc, parse);
            long end = System.currentTimeMillis();
            LOG.debug("ParseFilter {} took {} msec", filter.getClass()
                    .getName(), end - start);
        }
    }
}
