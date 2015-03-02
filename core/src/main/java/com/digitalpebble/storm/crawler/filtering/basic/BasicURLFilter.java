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

package com.digitalpebble.storm.crawler.filtering.basic;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.filtering.URLFilter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicURLFilter implements URLFilter {

    private static final Logger LOG = LoggerFactory.getLogger(BasicURLFilter.class);

    boolean removeAnchorPart = true;
    final Set<String> queryElementsToRemove = new TreeSet<>();

    @Override
    public String filter(URL sourceUrl, Metadata sourceMetadata,
            String urlToFilter) {
        if (removeAnchorPart) {
            try {
                URL theURL = new URL(urlToFilter);
                String anchor = theURL.getRef();
                if (anchor != null)
                    urlToFilter = urlToFilter.replace("#" + anchor, "");
            } catch (MalformedURLException e) {
                return null;
            }
        }

        if (!queryElementsToRemove.isEmpty()) {
            urlToFilter = filterQueryElements(urlToFilter);
        }

        return urlToFilter;
    }

    @Override
    public void configure(Map stormConf, JsonNode paramNode) {
        JsonNode node = paramNode.get("removeAnchorPart");
        if (node != null) {
            removeAnchorPart = node.booleanValue();
        }

        node = paramNode.get("queryElementsToRemove");
        if (node != null) {
            if (!node.isArray()) {
                LOG.warn("Failed to configure queryElementsToRemove.  Not an array: {}",
                        node.toString());
            } else {
                ArrayNode array = (ArrayNode) node;
                for (JsonNode element : array) {
                    queryElementsToRemove.add(element.asText());
                }
            }
        }
    }

    /**
     * Basic filter to remove query parameters from urls so parameters that don't change the content
     * of the page can be removed. An example would be a google analytics query parameter like
     * "utm_campaign" which might have several different values for a url that points to the same
     * content.
     */
    private String filterQueryElements(String urlToFilter) {
        URIBuilder uriBuilder;
        try {
            // Handle illegal characters by making a url first
            // this will clean illegal characters like |
            URL url = new URL(urlToFilter);
            // this constructor will encode illegal characters.
            URI uri =
                    new URI(url.getProtocol(), url.getAuthority(), url.getPath(), url.getQuery(),
                            url.getRef());
            uriBuilder = new URIBuilder(uri);
        } catch (URISyntaxException | MalformedURLException e) {
            LOG.warn("Invalid urlToFilter {}. {}", urlToFilter, e);
            return null;
        }

        List<NameValuePair> params = uriBuilder.getQueryParams();
        List<NameValuePair> paramsToKeep = new ArrayList<>();
        for (NameValuePair param : params) {
            if (!queryElementsToRemove.contains(param.getName())) {
                paramsToKeep.add(param);
            }
        }
        uriBuilder.removeQuery();
        Collections.sort(paramsToKeep, comp);
        for (NameValuePair param : paramsToKeep) {
            uriBuilder.addParameter(param.getName(), param.getValue());
        }

        return uriBuilder.toString();
    }

    Comparator<NameValuePair> comp = new Comparator<NameValuePair>() {
        @Override
        public int compare(NameValuePair p1, NameValuePair p2) {
            return p1.getName().compareTo(p2.getName());
        }
    };
}
