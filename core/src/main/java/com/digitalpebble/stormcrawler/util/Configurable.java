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
package com.digitalpebble.stormcrawler.util;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;

public interface Configurable {
    /**
     * Called when this filter is being initialized
     *
     * @param stormConf The Storm configuration used for the ParserBolt
     * @param filterParams the filter specific configuration. Never null
     */
    default void configure(Map stormConf, JsonNode filterParams) {}

    /** Replace with ConfigurableUtil.configure */
    @Deprecated
    public static <T extends Configurable> List<T> configure(
            Map stormConf, JsonNode filtersConf, Class<T> filterClass, String callingClass) {
        return ConfigurableUtil.configure(stormConf, filtersConf, filterClass, callingClass);
    }
}
