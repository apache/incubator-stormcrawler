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

package com.digitalpebble.stormcrawler.filtering.basic;

import java.net.URL;
import java.util.Map;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.filtering.URLFilter;
import com.fasterxml.jackson.databind.JsonNode;

/** Filters links to self **/
public class SelfURLFilter implements URLFilter {

    @Override
    public String filter(URL sourceUrl, Metadata sourceMetadata,
            String urlToFilter) {

        if (sourceUrl == null) {
            return urlToFilter;
        }

        if (sourceUrl.toExternalForm().equalsIgnoreCase(urlToFilter))
            return null;

        return urlToFilter;
    }

    @Override
    public void configure(Map stormConf, JsonNode paramNode) {

    }

}
