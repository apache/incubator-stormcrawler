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

package com.digitalpebble.stormcrawler.protocol.jbrowser;

import java.util.logging.Level;

import org.apache.storm.Config;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.protocol.AbstractHttpProtocol;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.machinepublishers.jbrowserdriver.JBrowserDriver;
import com.machinepublishers.jbrowserdriver.Settings;
import com.machinepublishers.jbrowserdriver.Settings.Builder;
import com.machinepublishers.jbrowserdriver.UserAgent;
import com.machinepublishers.jbrowserdriver.UserAgent.Family;

import crawlercommons.robots.BaseRobotRules;

/**
 * Uses JBrowserdriver to handle http and https. To activate, specify the
 * following in the configuration : http.protocol.implementation:
 * <dl>
 * <dt>http.protocol.implementation</dt>
 * <dd>com.digitalpebble.stormcrawler.protocol.jbrowser.HttpProtocol
 * <dd>
 * <dt>https.protocol.implementation</dt>
 * <dd>com.digitalpebble.stormcrawler.protocol.jbrowser.HttpProtocol
 * <dd>
 * </dl>
 * There is only one instance of HttpProtocol per ProtocolFactory.
 **/

public class HttpProtocol extends AbstractHttpProtocol {

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(HttpProtocol.class);

    private ThreadLocal<JBrowserDriver> driver;

    private NavigationFilters filters;

    @Override
    public void configure(Config conf) {
        super.configure(conf);

        int timeout = ConfUtils.getInt(conf, "http.timeout", 10000);

        Builder settings = Settings.builder().headless(true);
        settings.connectionReqTimeout(timeout);
        settings.ajaxResourceTimeout(timeout);
        settings.connectTimeout(timeout);
        settings.socketTimeout(timeout);

        String userAgentString = getAgentString(
                ConfUtils.getString(conf, "http.agent.name"),
                ConfUtils.getString(conf, "http.agent.version"),
                ConfUtils.getString(conf, "http.agent.description"),
                ConfUtils.getString(conf, "http.agent.url"),
                ConfUtils.getString(conf, "http.agent.email"));

        UserAgent agent = new UserAgent(Family.MOZILLA, "", "", "", "",
                userAgentString);
        settings.userAgent(agent);

        // TODO set proxy?

        // TODO max route connections

        // allow up to 10 connections or same as the number of threads used for
        // fetching
        int maxFetchThreads = ConfUtils.getInt(conf, "fetcher.threads.number",
                10);
        settings.maxConnections(maxFetchThreads);

        settings.loggerLevel(Level.OFF);

        // risks of memory leaks?
        driver = new ThreadLocal<JBrowserDriver>() {
            @Override
            protected JBrowserDriver initialValue() {
                long start = System.currentTimeMillis();
                JBrowserDriver d = new JBrowserDriver(settings.build());
                long end = System.currentTimeMillis();
                LOG.info("JBrowserDriver instanciated in {} msec",
                        (end - start));
                return d;
            }
        };

        filters = NavigationFilters.fromConf(conf);
    }

    public ProtocolResponse getProtocolOutput(String url, Metadata metadata)
            throws Exception {
        // This will block for the page load and any
        // associated AJAX requests
        driver.get().get(url);

        // call the filters
        ProtocolResponse response = filters.filter(driver.get(), metadata);
        if (response != null)
            return response;

        // if no filters got triggered
        byte[] content = driver.get().getPageSource().getBytes();
        return new ProtocolResponse(content, driver.get().getStatusCode(),
                metadata);
    }

    public static void main(String args[]) throws Exception {
        HttpProtocol protocol = new HttpProtocol();
        Config conf = new Config();

        String url = args[0];

        ConfUtils.loadConf(args[1], conf);
        protocol.configure(conf);

        if (!protocol.skipRobots) {
            BaseRobotRules rules = protocol.getRobotRules(url);
            System.out.println("is allowed : " + rules.isAllowed(url));
        }

        Metadata md = new Metadata();
        long start = System.currentTimeMillis();
        ProtocolResponse response = protocol.getProtocolOutput(url, md);
        long timeFetching = System.currentTimeMillis() - start;
        System.out.println(url);
        System.out.println(response.getMetadata());
        System.out.println("status code: " + response.getStatusCode());
        System.out.println("content length: " + response.getContent().length);
        System.out.println("fetched in : " + timeFetching + " msec");
        System.exit(0);
    }

}