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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import com.digitalpebble.storm.crawler.util.ConfUtils;

import crawlercommons.url.PaidLevelDomain;

/**
 * Utility class which encapsulates the filtering of URLs based on the hostname
 * or domain of the source URL.
 **/
public class URLFilterUtil {

    private boolean ignoreOutsideHost;
    private boolean ignoreOutsideDomain;

    private String fromHost;
    private String fromDomain;
    private URL parentURL;

    /**
     * @param configuration
     *            of the topology
     * **/
    public URLFilterUtil(Map<String, Object> conf) {
        ignoreOutsideHost = ConfUtils.getBoolean(conf,
                "parser.ignore.outlinks.outside.host", false);
        ignoreOutsideDomain = ConfUtils.getBoolean(conf,
                "parser.ignore.outlinks.outside.domain", false);
    }

    public void setSourceURL(URL sourceURL) {
        parentURL = sourceURL;
        fromHost = parentURL.getHost().toLowerCase();
        fromDomain = PaidLevelDomain.getPLD(fromHost);
    }

    /**
     * @param target
     *            URL
     * @return boolean value indicating whether the URL should be kept given the
     *         configuration and source URL
     ***/
    public boolean filter(String targetURL) {
        // do not filter
        if (!ignoreOutsideHost && !ignoreOutsideDomain)
            return true;

        URL tURL;
        try {
            tURL = new URL(targetURL);
        } catch (MalformedURLException e1) {
            return false;
        }

        // resolve the host of the target
        String toHost = tURL.getHost();

        if (ignoreOutsideHost) {
            if (toHost == null || !toHost.equals(fromHost)) {
                return false;
            }
        }

        if (ignoreOutsideDomain) {
            String toDomain;
            try {
                toDomain = PaidLevelDomain.getPLD(toHost);
            } catch (Exception e) {
                toDomain = null;
            }
            if (toDomain == null || !toDomain.equals(fromDomain)) {
                return false;
            }
        }

        return true;
    }

}
