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
package com.digitalpebble.stormcrawler.protocol;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.util.ConfUtils;

import crawlercommons.robots.BaseRobotRules;

public abstract class AbstractHttpProtocol implements Protocol {

    private com.digitalpebble.stormcrawler.protocol.HttpRobotRulesParser robots;

    protected boolean skipRobots = false;

    protected boolean storeHTTPHeaders = false;
    
    protected boolean useCookies = false;

    @Override
    public void configure(Config conf) {
        this.skipRobots = ConfUtils.getBoolean(conf, "http.skip.robots", false);
        this.storeHTTPHeaders = ConfUtils.getBoolean(conf,
                "http.store.headers", false);
        this.useCookies = ConfUtils.getBoolean(conf,
                "http.use.cookies", false);
        robots = new HttpRobotRulesParser(conf);
    }

    @Override
    public BaseRobotRules getRobotRules(String url) {
        if (this.skipRobots)
            return RobotRulesParser.EMPTY_RULES;
        return robots.getRobotRulesSet(this, url);
    }

    @Override
    public void cleanup() {
    }

    public static String getAgentString(Config conf) {
        return getAgentString(ConfUtils.getString(conf, "http.agent.name"),
                ConfUtils.getString(conf, "http.agent.version"),
                ConfUtils.getString(conf, "http.agent.description"),
                ConfUtils.getString(conf, "http.agent.url"),
                ConfUtils.getString(conf, "http.agent.email"));
    }

    protected static String getAgentString(String agentName,
            String agentVersion, String agentDesc, String agentURL,
            String agentEmail) {

        StringBuilder buf = new StringBuilder();

        buf.append(agentName);

        if (StringUtils.isNotBlank(agentVersion)) {
            buf.append("/");
            buf.append(agentVersion);
        }

        boolean hasAgentDesc = StringUtils.isNotBlank(agentDesc);
        boolean hasAgentURL = StringUtils.isNotBlank(agentURL);
        boolean hasAgentEmail = StringUtils.isNotBlank(agentEmail);

        if (hasAgentDesc || hasAgentEmail || hasAgentURL) {
            buf.append(" (");

            if (hasAgentDesc) {
                buf.append(agentDesc);
                if (hasAgentURL || hasAgentEmail)
                    buf.append("; ");
            }

            if (hasAgentURL) {
                buf.append(agentURL);
                if (hasAgentEmail)
                    buf.append("; ");
            }

            if (hasAgentEmail) {
                buf.append(agentEmail);
            }

            buf.append(")");
        }

        return buf.toString();
    }

    /** Called by extensions of this class **/
    protected static void main(AbstractHttpProtocol protocol, String args[])
            throws Exception {
        Config conf = new Config();

        ConfUtils.loadConf(args[0], conf);
        protocol.configure(conf);

        Set<Runnable> threads = new HashSet<>();

        class Fetchable implements Runnable {
            String url;

            Fetchable(String url) {
                this.url = url;
            }

            public void run() {

                StringBuilder stringB = new StringBuilder();
                stringB.append(url).append("\n");

                if (!protocol.skipRobots) {
                    BaseRobotRules rules = protocol.getRobotRules(url);
                    stringB.append("is allowed : ")
                            .append(rules.isAllowed(url));
                }

                Metadata md = new Metadata();
                long start = System.currentTimeMillis();
                ProtocolResponse response;
                try {
                    response = protocol.getProtocolOutput(url, md);
                    stringB.append(response.getMetadata()).append("\n");
                    stringB.append("status code: " + response.getStatusCode())
                            .append("\n");
                    stringB.append(
                            "content length: " + response.getContent().length)
                            .append("\n");
                    long timeFetching = System.currentTimeMillis() - start;
                    stringB.append("fetched in : " + timeFetching + " msec");
                    System.out.println(stringB);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    threads.remove(this);
                }
            }
        }

        for (int i = 1; i < args.length; i++) {
            Fetchable p = new Fetchable(args[i]);
            threads.add(p);
            new Thread(p).start();
        }

        while (threads.size() > 0) {
            Thread.sleep(1000);
        }

        protocol.cleanup();
        System.exit(0);
    }

}
