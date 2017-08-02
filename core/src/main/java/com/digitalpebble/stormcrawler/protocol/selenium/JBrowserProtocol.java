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

package com.digitalpebble.stormcrawler.protocol.selenium;

import java.util.logging.Level;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.openqa.selenium.remote.RemoteWebDriver;

import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.machinepublishers.jbrowserdriver.JBrowserDriver;
import com.machinepublishers.jbrowserdriver.ProxyConfig;
import com.machinepublishers.jbrowserdriver.Settings;
import com.machinepublishers.jbrowserdriver.Settings.Builder;
import com.machinepublishers.jbrowserdriver.UserAgent;
import com.machinepublishers.jbrowserdriver.UserAgent.Family;

/**
 * Uses JBrowserdriver to handle http and https. To activate, specify the
 * following in the configuration : http.protocol.implementation:
 * <dl>
 * <dt>http.protocol.implementation</dt>
 * <dd>com.digitalpebble.stormcrawler.protocol.selenium.JBrowserProtocol
 * <dd>
 * <dt>https.protocol.implementation</dt>
 * <dd>com.digitalpebble.stormcrawler.protocol.selenium.JBrowserProtocol
 * <dd>
 * </dl>
 * Note : there is only one instance of HttpProtocol per ProtocolFactory, as
 * such, it gets called from multiple threads. This class creates a number of
 * servers on the machine where it runs and sends the calls to each instance
 * separately.
 **/
public class JBrowserProtocol extends SeleniumProtocol {

    private static String JBROWSER_NUM_PROCESSES_PARAM = "jbrowser.num.processes";

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

        // Instantiate one driver per process
        long start = System.currentTimeMillis();
        for (int i = 0; i < numProc; i++) {
            JBrowserDriver d = new JBrowserDriver(settings.build());
            drivers.add(d);
        }
        long end = System.currentTimeMillis();
        LOG.info("{} JBrowserDriver(s) instanciated in {} msec", numProc,
                (end - start));
    }

    @Override
    public void cleanup() {
        for (RemoteWebDriver driver : drivers) {
            try {
                ((JBrowserDriver) driver).close();
            } catch (Exception e) {
            }
        }
    }

    public static void main(String[] args) throws Exception {
        JBrowserProtocol.main(new JBrowserProtocol(), args);
    }

}
