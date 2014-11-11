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

package com.digitalpebble.storm.crawler.filtering.domain;

import org.codehaus.jackson.JsonNode;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.net.URL;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.storm.crawler.filtering.URLFilter;
import com.digitalpebble.storm.crawler.util.URLUtil;
import com.digitalpebble.storm.crawler.util.domain.DomainSuffix;

//  Borrowed from Apache Nutch 1.9

public class DomainBlacklistURLFilter implements URLFilter {

    private Set<String> domainSet = new LinkedHashSet<String>();
    private final static Logger LOG = LoggerFactory.getLogger(DomainBlacklistURLFilter.class);

    public void configure(JsonNode paramNode) {

        JsonNode filenameNode = paramNode.get("domainBlacklistFile");
        String rulesFileName;
        if (filenameNode != null) {
            rulesFileName = filenameNode.getTextValue();
        } else {
            rulesFileName = "default-domain-blacklist.txt";
        }
        this.domainSet = readRules(rulesFileName);
    }

    public String filter(String url) {
        try {
            // match for suffix, domain, and host in that order.  more general will
            // override more specific

            String domain = URLUtil.getDomainName(url).toLowerCase().trim();
            String host = URLUtil.getHost(url);
            String suffix = null;
            DomainSuffix domainSuffix = URLUtil.getDomainSuffix(url);

            if (domainSuffix != null) {
                suffix = domainSuffix.getDomain();
            }

            if (domainSet.contains(suffix) || domainSet.contains(domain)
                    || domainSet.contains(host)) {
                // Matches, filter!
                return null;
            }

            // doesn't match, allow
            return url;
        }
        catch (Exception e) {

            // if an error happens, allow the url to pass
            LOG.error("Could not apply filter on url: " + url + "\n"
                    + e.toString());
            return null;
        }
    }

    private Set<String> readRules(String rulesFileName) {
        Set<String> rules = new LinkedHashSet<String>();
        try {

            InputStream regexStream = getClass().getClassLoader().getResourceAsStream(rulesFileName);
            Reader reader = new InputStreamReader(regexStream, "UTF-8");
            BufferedReader in = new BufferedReader(reader);
            String line;

            while ((line = in.readLine()) != null) {
                if (line.length() == 0) {
                    continue;
                }
                char first = line.charAt(0);
                switch (first) {

                    case ' ':
                    case '\n':
                    case '#':           // skip blank & comment lines
                        continue;
                }

                String domain = line;
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Adding rule [" + domain + "]");
                }

                rules.add(domain);

            }
        } catch (IOException e) {
            LOG.error("There was an error reading the default-domain-blacklist file");
            e.printStackTrace();
        }

        return rules;
    }
}
