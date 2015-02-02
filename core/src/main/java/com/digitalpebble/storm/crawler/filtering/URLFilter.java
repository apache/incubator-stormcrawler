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

import java.net.URL;
import java.util.Map;

import com.digitalpebble.storm.crawler.Metadata;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Unlike Nutch, URLFilters can normalise the URLs as well as filtering them.
 * URLFilter instances should be used via URLFilters
 */
public interface URLFilter {

    /**
     * Called when this filter is being initialized
     * 
     * @param stormConf
     *            The Storm configuration used for the ParserBolt
     * @param filterParams
     *            the filter specific configuration. Never null
     */
    public void configure(Map stormConf, JsonNode filterParams);

    /**
     * Returns null if the URL is to be removed or a normalised representation
     * which can correspond to the input URL
     * 
     * @param sourceUrl
     *            the URL of the page where the URL was found. Can be null.
     * @param sourceMetadata
     *            the metadata collected for the page
     * @param urlToFilter
     *            the URL to be filtered
     */
    public String filter(URL sourceUrl, Metadata sourceMetadata,
            String urlToFilter);
}
