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

package com.digitalpebble.stormcrawler.filtering.depth;

import java.net.URL;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.filtering.URLFilter;
import com.digitalpebble.stormcrawler.util.MetadataTransfer;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Filter out URLs whose depth is greater than maxDepth.
 */
public class MaxDepthFilter implements URLFilter {

    private static final Logger LOG = LoggerFactory
            .getLogger(MaxDepthFilter.class);

    private int maxDepth;

    @Override
    public void configure(Map stormConf, JsonNode paramNode) {
        JsonNode node = paramNode.get("maxDepth");
        if (node != null && node.isInt()) {
            maxDepth = node.intValue();
        } else {
            maxDepth = 0;
            LOG.warn("maxDepth paramater not found");
        }
    }

    @Override
    public String filter(URL pageUrl, Metadata sourceMetadata, String url) {
        int depth = getDepth(sourceMetadata);
        if (maxDepth > 0 && depth >= maxDepth) {
            return null;
        }
        return url;
    }

    private int getDepth(Metadata sourceMetadata) {
        String depth = sourceMetadata
                .getFirstValue(MetadataTransfer.depthKeyName);
        if (StringUtils.isNumeric(depth)) {
            return Integer.parseInt(depth);
        } else {
            return -1;
        }
    }
}
