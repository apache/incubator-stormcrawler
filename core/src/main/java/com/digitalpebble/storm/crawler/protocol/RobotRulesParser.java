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

package com.digitalpebble.storm.crawler.protocol;

import java.net.URL;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.StringTokenizer;

import javax.security.auth.login.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

import com.digitalpebble.storm.crawler.util.ConfUtils;

import crawlercommons.robots.BaseRobotRules;
import crawlercommons.robots.SimpleRobotRules;
import crawlercommons.robots.SimpleRobotRules.RobotRulesMode;
import crawlercommons.robots.SimpleRobotRulesParser;

/**
 * This class uses crawler-commons for handling the parsing of
 * {@code robots.txt} files. It emits SimpleRobotRules objects, which describe
 * the download permissions as described in SimpleRobotRulesParser.
 */
public abstract class RobotRulesParser {

    public static final Logger LOG = LoggerFactory
            .getLogger(RobotRulesParser.class);

    protected static final Hashtable<String, BaseRobotRules> CACHE = new Hashtable<String, BaseRobotRules>();

    /**
     * A {@link BaseRobotRules} object appropriate for use when the
     * {@code robots.txt} file is empty or missing; all requests are allowed.
     */
    public static final BaseRobotRules EMPTY_RULES = new SimpleRobotRules(
            RobotRulesMode.ALLOW_ALL);

    /**
     * A {@link BaseRobotRules} object appropriate for use when the
     * {@code robots.txt} file is not fetched due to a {@code 403/Forbidden}
     * response; all requests are disallowed.
     */
    public static BaseRobotRules FORBID_ALL_RULES = new SimpleRobotRules(
            RobotRulesMode.ALLOW_NONE);

    private static SimpleRobotRulesParser robotParser = new SimpleRobotRulesParser();
    protected String agentNames;

    public RobotRulesParser() {
    }

    /**
     * Set the {@link Configuration} object
     */
    public void setConf(Config conf) {

        // Grab the agent names we advertise to robots files.
        String agentName = ConfUtils.getString(conf, "http.agent.name");
        if (null == agentName) {
            throw new RuntimeException("Agent name not configured!");
        }

        String agentNames = ConfUtils.getString(conf, "http.robots.agents", "");
        StringTokenizer tok = new StringTokenizer(agentNames, ",");
        ArrayList<String> agents = new ArrayList<String>();
        while (tok.hasMoreTokens()) {
            agents.add(tok.nextToken().trim());
        }

        /**
         * If there are no agents for robots-parsing, use the default
         * agent-string. If both are present, our agent-string should be the
         * first one we advertise to robots-parsing.
         */
        if (agents.size() == 0) {
            LOG.info(
                    "No agents listed in 'http.robots.agents' property! Using http.agent.name [{}]",
                    agentName);
            this.agentNames = agentName;
        } else {
            StringBuffer combinedAgentsString = new StringBuffer(agentName);
            int index = 0;

            if ((agents.get(0)).equalsIgnoreCase(agentName))
                index++;
            else
                LOG.info(
                        "Agent we advertise ({}) not listed first in 'http.robots.agents' property!",
                        agentName);

            // append all the agents from the http.robots.agents property
            for (; index < agents.size(); index++) {
                combinedAgentsString.append(", " + agents.get(index));
            }

            this.agentNames = combinedAgentsString.toString();
        }
    }

    /**
     * Parses the robots content using the {@link SimpleRobotRulesParser} from
     * crawler commons
     * 
     * @param url
     *            A string containing url
     * @param content
     *            Contents of the robots file in a byte array
     * @param contentType
     *            The
     * @param robotName
     *            A string containing value of
     * @return BaseRobotRules object
     */
    public BaseRobotRules parseRules(String url, byte[] content,
            String contentType, String robotName) {
        return robotParser.parseContent(url, content, contentType, robotName);
    }

    public BaseRobotRules getRobotRulesSet(Protocol protocol, String url) {
        URL u = null;
        try {
            u = new URL(url);
        } catch (Exception e) {
            return EMPTY_RULES;
        }
        return getRobotRulesSet(protocol, u);
    }

    public abstract BaseRobotRules getRobotRulesSet(Protocol protocol, URL url);

}
