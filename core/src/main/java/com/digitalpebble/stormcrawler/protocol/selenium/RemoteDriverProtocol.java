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
package com.digitalpebble.stormcrawler.protocol.selenium;

import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.URLResolver;
import com.digitalpebble.stormcrawler.util.URLResolver.ResolvedUrl;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.storm.Config;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.openqa.selenium.WebDriver.Timeouts;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;

/**
 * Delegates the requests to one or more remote selenium servers. The processes must be started /
 * stopped separately. The URLs to connect to are specified with the config 'selenium.addresses'.
 *
 * <p>Is configured like this:
 *
 * <pre>
 *   selenium.addresses:
 *     # Similar to the second address
 *     - http://first-address:4444
 *     - address: http://second-address:4444
 *     - address: http://second-address:4444
 *       resolve: NOTHING
 *     - address: http://third-address:4444
 *       resolve: IP
 *     - address: http://fourth-address:4444
 *       resolve: IPv4
 *     - address: http://fifth-address:4444
 *       resolve: IPv6
 *     - address: http://first-IP-address:4444
 *       resolve: HOSTNAME
 *     - address: http://second-IP-address:4444
 *       resolve: CANONICAL_HOSTNAME
 *   selenium.implicitlyWait: 1000000
 *   selenium.pageLoadTimeout: 1000000
 *   selenium.scriptTimeout: 1000000
 *   selenium.capabilities:
 *     takesScreenshot: true
 *     loadImages: true
 *     javascriptEnabled: true
 *     browserName: chrome
 * </pre>
 *
 * @see URLResolver for possible resolveTo targets.
 */
public class RemoteDriverProtocol extends SeleniumProtocol {

    public static final String SELENIUM_CAPABILITIES = "selenium.capabilities";
    public static final String SELENIUM_ADDRESSES = "selenium.addresses";
    public static final String SELENIUM_IMPLICIT_WAIT = "selenium.implicitlyWait";
    public static final String SELENIUM_PAYLOAD_TIMEOUT = "selenium.pageLoadTimeout";
    public static final String SELENIUM_SCRIPT_TIMEOUT = "selenium.scriptTimeout";

    @Nullable
    public static List<ResolvedUrl> loadURLsFromConfig(@NotNull Config conf)
            throws MalformedURLException {
        Collection<Object> collection = ConfUtils.loadCollectionOrNull(conf, SELENIUM_ADDRESSES);
        if (collection == null) return null;
        List<ResolvedUrl> retVal = new ArrayList<>(collection.size());
        for (Object entry : collection) {
            ResolvedUrl urlTuple;
            if (entry instanceof String) {
                urlTuple = ResolvedUrl.of(new URL((String) entry));
            } else if (entry instanceof Map<?, ?>) {
                //noinspection unchecked
                Map<String, Object> subConfig = (Map<String, Object>) entry;
                String address = ConfUtils.getString(subConfig, "address");
                if (address == null) {
                    throw new RuntimeException(
                            String.format(
                                    "The config for a %s entry is missing an 'address' field.",
                                    SELENIUM_ADDRESSES));
                }

                URL origin = new URL(address);

                URLResolver strategy =
                        ConfUtils.getEnumOrDefault(subConfig, "resolve", null, URLResolver.class);

                if (strategy != null) {
                    urlTuple = strategy.resolve(origin);
                } else {
                    urlTuple = ResolvedUrl.of(origin);
                }
            } else {
                throw new RuntimeException(
                        String.format("Unsupported entry at %s", SELENIUM_ADDRESSES));
            }
            retVal.add(urlTuple);
        }
        return retVal;
    }

    @Override
    public void configure(@NotNull Config conf) {
        super.configure(conf);

        // see https://github.com/SeleniumHQ/selenium/wiki/DesiredCapabilities
        DesiredCapabilities capabilities = new DesiredCapabilities();
        capabilities.setJavascriptEnabled(true);

        String userAgentString = getAgentString(conf);

        // custom capabilities
        Map<String, Object> confCapabilities = ConfUtils.getSubConfig(conf, SELENIUM_CAPABILITIES);
        if (confCapabilities != null) {
            for (Entry<String, Object> entry : confCapabilities.entrySet()) {
                Object val = entry.getValue();
                // substitute variable $useragent for the real value
                if (val instanceof String && "$useragent".equalsIgnoreCase(val.toString())) {
                    val = userAgentString;
                }
                capabilities.setCapability(entry.getKey(), val);
            }
        }

        List<ResolvedUrl> urls;
        try {
            urls = loadURLsFromConfig(conf);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }

        // load adresses from config
        if (urls == null) {
            throw new RuntimeException(String.format("No value found for %s", SELENIUM_ADDRESSES));
        }

        for (ResolvedUrl resolvedUrl : urls) {
            if (resolvedUrl.wasNotSuccessfullyResolved()) {
                LOG.warn("The url {} was not successfully resolved.", resolvedUrl.getOrigin());
            }
            LOG.info("Create RemoteWebDriver for: {}", resolvedUrl);
            RemoteWebDriver driver =
                    new RemoteWebDriver(resolvedUrl.getResolvedOrOrigin(), capabilities);
            Timeouts touts = driver.manage().timeouts();
            int implicitWait = ConfUtils.getInt(conf, SELENIUM_IMPLICIT_WAIT, 0);
            int pageLoadTimeout = ConfUtils.getInt(conf, SELENIUM_PAYLOAD_TIMEOUT, 0);
            int scriptTimeout = ConfUtils.getInt(conf, SELENIUM_SCRIPT_TIMEOUT, 0);
            touts.implicitlyWait(Duration.ofMillis(implicitWait));
            touts.pageLoadTimeout(Duration.ofMillis(pageLoadTimeout));
            touts.scriptTimeout(Duration.ofMillis(scriptTimeout));
            drivers.add(driver);
        }
    }

    //    @TestOnly
    //    public static void main(String[] args) throws Exception {
    //        RemoteDriverProtocol.mainForTest(new RemoteDriverProtocol(), args);
    //    }
}
