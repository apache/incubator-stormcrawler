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

// JDK imports
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;

// Commons Logging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.storm.crawler.filtering.URLFilter;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * An abstract class for implementing Regex URL filtering. Adapted from Apache
 * Nutch 1.9
 * 
 */

public abstract class RegexURLFilterBase implements URLFilter {

    /** logger */
    private final static Logger LOG = LoggerFactory
            .getLogger(RegexURLFilterBase.class);

    /** A list of applicable rules */
    private List<RegexRule> rules;

    public void configure(JsonNode paramNode) {

        JsonNode filenameNode = paramNode.get("regexFilterFile");
        String rulesFileName;
        if (filenameNode != null) {
            rulesFileName = filenameNode.textValue();
        } else {
            rulesFileName = "default-regex-filters.txt";
        }
        this.rules = readRules(rulesFileName);
    }

    private List<RegexRule> readRules(String rulesFile) {
        List<RegexRule> rules = new ArrayList<RegexRule>();

        try {

            InputStream regexStream = getClass().getClassLoader()
                    .getResourceAsStream(rulesFile);
            Reader reader = new InputStreamReader(regexStream, "UTF-8");
            BufferedReader in = new BufferedReader(reader);
            String line;

            while ((line = in.readLine()) != null) {
                if (line.length() == 0) {
                    continue;
                }
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
                    continue;
                default:
                    throw new IOException("Invalid first character: " + line);
                }

                String regex = line.substring(1);
                LOG.trace("Adding rule [{}]", regex);
                RegexRule rule = createRule(sign, regex);
                rules.add(rule);

            }
        } catch (IOException e) {
            LOG.error("There was an error reading the default-regex-filters file");
            e.printStackTrace();
        }
        return rules;
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

    // Inherited Javadoc
    public String filter(String url) {
        for (RegexRule rule : rules) {
            if (rule.match(url)) {
                return rule.accept() ? url : null;
            }
        }
        ;
        return null;
    }

}
