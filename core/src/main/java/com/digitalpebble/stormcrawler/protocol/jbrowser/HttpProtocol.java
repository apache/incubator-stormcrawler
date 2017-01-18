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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.protocol.AbstractHttpProtocol;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.machinepublishers.jbrowserdriver.JBrowserDriver;
import com.machinepublishers.jbrowserdriver.ProxyConfig;
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

    private static String JBROWSER_NUM_PROCESSES_PARAM = "jbrowser.num.processes";

    private LinkedBlockingQueue<JBrowserDriver> drivers;

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

        String proxyHost = ConfUtils.getString(conf, "http.proxy.host", null);
        int proxyPort = ConfUtils.getInt(conf, "http.proxy.port", 8080);
        if (StringUtils.isNotBlank(proxyHost)) {
            ProxyConfig proxy = new ProxyConfig(ProxyConfig.Type.HTTP,
                    proxyHost, proxyPort);
            settings.proxy(proxy);
        }

        // max route connections
        settings.maxRouteConnections(20);

        // allow up to 10 connections or same as the number of threads used for
        // fetching
        int maxFetchThreads = ConfUtils.getInt(conf, "fetcher.threads.number",
                10);
        settings.maxConnections(maxFetchThreads);

        settings.loggerLevel(Level.OFF);

        settings.blockAds(true);
        settings.ignoreDialogs(true);
        settings.quickRender(true);

        int numProc = ConfUtils.getInt(conf, JBROWSER_NUM_PROCESSES_PARAM, 5);

        // each driver instance is connected to a server instance running in a
        // separate JVM
        settings.processes(numProc);

        drivers = new LinkedBlockingQueue<>(numProc);

        // Instantiate one driver per process
        long start = System.currentTimeMillis();
        for (int i = 0; i < numProc; i++) {
            JBrowserDriver d = new JBrowserDriver(settings.build());
            drivers.add(d);
        }
        long end = System.currentTimeMillis();
        LOG.info("{} JBrowserDriver(s) instanciated in {} msec", numProc,
                (end - start));

        filters = NavigationFilters.fromConf(conf);
    }

    public ProtocolResponse getProtocolOutput(String url, Metadata metadata)
            throws Exception {
        // TODO check that the driver is not null
        JBrowserDriver driver = getDriver();
        try {
            // This will block for the page load and any
            // associated AJAX requests
            driver.get(url);

            // call the filters
            ProtocolResponse response = filters.filter(driver, metadata);
            if (response == null) {
                // if no filters got triggered
                byte[] content = driver.getPageSource().getBytes();
                int code = driver.getStatusCode();
                response = new ProtocolResponse(content, code, metadata);
            }
            return response;
        } finally {
            // finished with this driver - return it to the queue
            drivers.put(driver);
        }
    }

    /** Returns the first available driver **/
    private final JBrowserDriver getDriver() {
        JBrowserDriver d = null;
        try {
            d = drivers.take();
        } catch (InterruptedException e) {
        }
        return d;
    }

    @Override
    public void cleanup() {
        for (JBrowserDriver driver : drivers) {
            try {
                driver.close();
            } catch (Exception e) {
            }
        }
    }

    public static void main(String args[]) throws Exception {
        HttpProtocol protocol = new HttpProtocol();
        Config conf = new Config();

        ConfUtils.loadConf(args[0], conf);
        protocol.configure(conf);

        Set<Runnable> threads = new HashSet<Runnable>();

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
            Thread.currentThread().sleep(1000);
        }

        protocol.cleanup();
        System.exit(0);
    }

}