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

package com.digitalpebble.storm.crawler.filtering.regex;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.filtering.URLFilter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * An abstract class for implementing Regex URL filtering. Adapted from Apache
 * Nutch 1.9
 */
public abstract class RegexURLFilterBase implements URLFilter {

    private static final Logger LOG = LoggerFactory
            .getLogger(RegexURLFilterBase.class);

    /** A list of applicable rules */
    private List<RegexRule> rules;

    @Override
    public void configure(Map stormConf, JsonNode paramNode) {
        JsonNode node = paramNode.get("urlFilters");
        if (node != null && node.isArray()) {
            rules = readRules((ArrayNode) node);
        } else {
            JsonNode filenameNode = paramNode.get("regexFilterFile");
            String rulesFileName;
            if (filenameNode != null) {
                rulesFileName = filenameNode.textValue();
            } else {
                rulesFileName = "default-regex-filters.txt";
            }
            rules = readRules(rulesFileName);
        }
    }

    /** Populates a List of Rules off of JsonNode. */
    private List<RegexRule> readRules(ArrayNode rulesList) {
        List<RegexRule> rules = new ArrayList<RegexRule>();
        for (JsonNode urlFilterNode : rulesList) {
            try {
                RegexRule rule = createRule(urlFilterNode.asText());
                if (rule != null) {
                    rules.add(rule);
                }
            } catch (IOException e) {
                LOG.error("There was an error reading regex filter {}", urlFilterNode.asText(), e);
            }
        }
        return rules;
    }

    private List<RegexRule> readRules(String rulesFile) {
        List<RegexRule> rules = new ArrayList<RegexRule>();

        try {
            InputStream regexStream = getClass().getClassLoader()
                    .getResourceAsStream(rulesFile);
            Reader reader = new InputStreamReader(regexStream,
                    StandardCharsets.UTF_8);
            BufferedReader in = new BufferedReader(reader);
            String line;

            while ((line = in.readLine()) != null) {
                if (line.length() == 0) {
                    continue;
                }
                RegexRule rule = createRule(line);
                if (rule != null) {
                    rules.add(rule);
                }

            }
        } catch (IOException e) {
            LOG.error("There was an error reading the default-regex-filters file");
            e.printStackTrace();
        }
        return rules;
    }

    private RegexRule createRule(String line) throws IOException {
        char first = line.charAt(0);
        boolean sign = false;
        switch (first) {
        case '+':
            sign = true;
            break;
        case '-':
            sign = false;
            break;
        case ' ':
        case '\n':
        case '#': // skip blank & comment lines
            return null;
        default:
            throw new IOException("Invalid first character: " + line);
        }

        String regex = line.substring(1);
        LOG.trace("Adding rule [{}]", regex);
        RegexRule rule = createRule(sign, regex);
        return rule;
    }

    /**
     * Creates a new {@link RegexRule}.
     *
     * @param sign
     *            of the regular expression. A <code>true</code> value means
     *            that any URL matching this rule must be included, whereas a
     *            <code>false</code> value means that any URL matching this rule
     *            must be excluded.
     * @param regex
     *            is the regular expression associated to this rule.
     */
    protected abstract RegexRule createRule(boolean sign, String regex);

    /*
     * -------------------------- * <implementation:URLFilter> *
     * --------------------------
     */

    @Override
    public String filter(URL pageUrl, Metadata sourceMetadata, String url) {
        for (RegexRule rule : rules) {
            if (rule.match(url)) {
                return rule.accept() ? url : null;
            }
        }
        return null;
    }

}
