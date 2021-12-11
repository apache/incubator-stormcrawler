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
package com.digitalpebble.stormcrawler.protocol;

import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import crawlercommons.robots.BaseRobotRules;
import crawlercommons.robots.SimpleRobotRules;
import crawlercommons.robots.SimpleRobotRules.RobotRulesMode;
import crawlercommons.robots.SimpleRobotRulesParser;
import java.net.URL;
import java.util.ArrayList;
import java.util.StringTokenizer;
import javax.security.auth.login.Configuration;
import org.apache.storm.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class uses crawler-commons for handling the parsing of {@code robots.txt} files. It emits
 * SimpleRobotRules objects, which describe the download permissions as described in
 * SimpleRobotRulesParser.
 */
public abstract class RobotRulesParser {

    public static final Logger LOG = LoggerFactory.getLogger(RobotRulesParser.class);

    protected static Cache<String, RobotRules> CACHE;

    // if a server or client error happened while fetching the robots
    // cache the result for a shorter period before trying again
    protected static Cache<String, RobotRules> ERRORCACHE;

    /**
     * Parameter name to configure the cache for robots @see http://docs.guava-libraries.googlecode
     * .com/git/javadoc/com/google/common/cache/CacheBuilderSpec.html Default value is
     * "maximumSize=10000,expireAfterWrite=6h"
     */
    public static final String cacheConfigParamName = "robots.cache.spec";

    /**
     * Parameter name to configure the cache for robots errors @see
     * http://docs.guava-libraries.googlecode
     * .com/git/javadoc/com/google/common/cache/CacheBuilderSpec.html Default value is
     * "maximumSize=10000,expireAfterWrite=1h"
     */
    public static final String errorcacheConfigParamName = "robots.error.cache.spec";

    /**
     * A {@link BaseRobotRules} object appropriate for use when the {@code robots.txt} file is empty
     * or missing; all requests are allowed.
     */
    public static final BaseRobotRules EMPTY_RULES = new SimpleRobotRules(RobotRulesMode.ALLOW_ALL);

    /**
     * A {@link BaseRobotRules} object appropriate for use when the {@code robots.txt} file is not
     * fetched due to a {@code 403/Forbidden} response; all requests are disallowed.
     */
    public static final BaseRobotRules FORBID_ALL_RULES =
            new SimpleRobotRules(RobotRulesMode.ALLOW_NONE);

    private static final SimpleRobotRulesParser robotParser = new SimpleRobotRulesParser();

    static {
        robotParser.setMaxCrawlDelay(Long.MAX_VALUE);
    }

    protected String agentNames;

    public RobotRulesParser() {}

    /** Set the {@link Configuration} object */
    public void setConf(Config conf) {

        // Grab the agent names we advertise to robots files.
        String agentName = ConfUtils.getString(conf, "http.agent.name");
        if (null == agentName) {
            throw new RuntimeException("Agent name not configured!");
        }

        String configuredAgentNames = ConfUtils.getString(conf, "http.robots.agents", "");
        StringTokenizer tok = new StringTokenizer(configuredAgentNames, ",");
        ArrayList<String> agents = new ArrayList<>();
        while (tok.hasMoreTokens()) {
            agents.add(tok.nextToken().trim());
        }

        /*
         * If there are no agents for robots-parsing, use the default agent-string. If both are
         * present, our agent-string should be the first one we advertise to robots-parsing.
         */
        if (agents.isEmpty()) {
            LOG.info(
                    "No agents listed in 'http.robots.agents' property! Using http.agent.name [{}]",
                    agentName);
            this.agentNames = agentName;
        } else {
            int index = 0;
            if ((agents.get(0)).equalsIgnoreCase(agentName)) {
                index++;
            } else {
                LOG.info(
                        "Agent we advertise ({}) not listed first in 'http.robots.agents' property!",
                        agentName);
            }

            StringBuilder combinedAgentsString = new StringBuilder(agentName);
            // append all the agents from the http.robots.agents property
            for (; index < agents.size(); index++) {
                combinedAgentsString.append(", ").append(agents.get(index));
            }

            this.agentNames = combinedAgentsString.toString();
        }

        String spec =
                ConfUtils.getString(
                        conf, cacheConfigParamName, "maximumSize=10000,expireAfterWrite=6h");
        CACHE = Caffeine.from(spec).build();

        spec =
                ConfUtils.getString(
                        conf, errorcacheConfigParamName, "maximumSize=10000,expireAfterWrite=1h");
        ERRORCACHE = Caffeine.from(spec).build();
    }

    /**
     * Parses the robots content using the {@link SimpleRobotRulesParser} from crawler commons
     *
     * @param url A string containing url
     * @param content Contents of the robots file in a byte array
     * @param contentType The
     * @param robotName A string containing value of
     * @return BaseRobotRules object
     */
    public BaseRobotRules parseRules(
            String url, byte[] content, String contentType, String robotName) {
        return robotParser.parseContent(url, content, contentType, robotName);
    }

    public BaseRobotRules getRobotRulesSet(Protocol protocol, String url) {
        URL u;
        try {
            u = new URL(url);
        } catch (Exception e) {
            return EMPTY_RULES;
        }
        return getRobotRulesSet(protocol, u);
    }

    public abstract BaseRobotRules getRobotRulesSet(Protocol protocol, URL url);
}
