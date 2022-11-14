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
package com.digitalpebble.stormcrawler.filtering;

import com.digitalpebble.stormcrawler.JSONResource;
import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.Configurable;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

/**
 * Wrapper for the URLFilters defined in a JSON configuration.
 *
 * @see Configurable#createConfiguredInstance(Class, Class, Map, JsonNode) for more information.
 */
public class URLFilters extends URLFilter implements JSONResource {

    public static final URLFilters emptyURLFilters = new URLFilters();

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(URLFilters.class);

    private URLFilter[] filters;

    private URLFilters() {
        filters = new URLFilters[0];
    }

    private String configFile = "urlfilters.config.file";

    private Map<String, Object> stormConf;

    /**
     * Loads and configure the URLFilters based on the storm config if there is one otherwise
     * returns an empty URLFilter.
     */
    public static URLFilters fromConf(Map<String, Object> stormConf) {

        String configFile = ConfUtils.getString(stormConf, "urlfilters.config.file");
        if (StringUtils.isNotBlank(configFile)) {
            try {
                return new URLFilters(stormConf, configFile);
            } catch (IOException e) {
                String message = "Exception caught while loading the URLFilters from " + configFile;
                LOG.error(message);
                throw new RuntimeException(message, e);
            }
        }

        return URLFilters.emptyURLFilters;
    }

    /**
     * Loads the filters from a JSON configuration file
     *
     * @throws IOException
     */
    public URLFilters(Map<String, Object> stormConf, String configFile) throws IOException {
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
    public @Nullable String filter(
            @Nullable URL sourceUrl,
            @Nullable Metadata sourceMetadata,
            @NotNull String urlToFilter) {
        String normalizedURL = urlToFilter;
        try {
            for (URLFilter filter : filters) {
                long start = System.currentTimeMillis();
                normalizedURL = filter.filter(sourceUrl, sourceMetadata, normalizedURL);
                long end = System.currentTimeMillis();
                LOG.debug("URLFilter {} took {} msec", filter.getClass().getName(), end - start);
                if (normalizedURL == null) break;
            }
        } catch (Exception e) {
            LOG.error("URL filtering threw exception", e);
        }
        return normalizedURL;
    }

    @Override
    public String getResourceFile() {
        return this.configFile;
    }

    @Override
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode filtersConf) {
        List<URLFilter> list =
                Configurable.createConfiguredInstance(
                        this.getClass(), URLFilter.class, stormConf, filtersConf);
        filters = list.toArray(new URLFilter[0]);
    }
}
