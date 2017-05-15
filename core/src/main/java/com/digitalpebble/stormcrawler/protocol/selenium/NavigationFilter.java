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

import java.util.Map;

import org.openqa.selenium.remote.RemoteWebDriver;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import com.fasterxml.jackson.databind.JsonNode;

public abstract class NavigationFilter {
    /**
     * Called when this filter is being initialised
     * 
     * @param stormConf
     *            The Storm configuration used for the parsing bolt
     * @param filterParams
     *            the filter specific configuration. Never null
     */
    public void configure(@SuppressWarnings("rawtypes") Map stormConf,
            JsonNode filterParams) {
    }

    /** The end result comes from the first filter to return non-null **/
    public abstract ProtocolResponse filter(RemoteWebDriver driver,
            Metadata metadata);
}
