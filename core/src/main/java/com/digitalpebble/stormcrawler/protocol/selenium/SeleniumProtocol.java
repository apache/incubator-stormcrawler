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

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.Config;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.protocol.AbstractHttpProtocol;
import com.digitalpebble.stormcrawler.protocol.HttpHeaders;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;

public abstract class SeleniumProtocol extends AbstractHttpProtocol {

    protected static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(SeleniumProtocol.class);

    protected LinkedBlockingQueue<RemoteWebDriver> drivers;

    private NavigationFilters filters;

    @Override
    public void configure(Config conf) {
        super.configure(conf);
        drivers = new LinkedBlockingQueue<>();
        filters = NavigationFilters.fromConf(conf);
    }

    public ProtocolResponse getProtocolOutput(String url, Metadata metadata)
            throws Exception {
        RemoteWebDriver driver = null;
        while ((driver = getDriver()) == null) {
        }
        try {
            // This will block for the page load and any
            // associated AJAX requests
            driver.get(url);

            String u = driver.getCurrentUrl();

            // if the URL is different then we must have hit a redirection
            if (!u.equalsIgnoreCase(url)) {
                byte[] content = new byte[] {};
                Metadata m = new Metadata();
                m.addValue(HttpHeaders.LOCATION, u);
                return new ProtocolResponse(content, 307, m);
            }

            // call the filters
            ProtocolResponse response = filters.filter(driver, metadata);
            if (response == null) {
                // if no filters got triggered
                byte[] content = driver.getPageSource().getBytes();
                response = new ProtocolResponse(content, 200, new Metadata());
            }
            return response;
        } finally {
            // finished with this driver - return it to the queue
            drivers.put(driver);
        }
    }

    /** Returns the first available driver **/
    private final RemoteWebDriver getDriver() {
        try {
            return drivers.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return null;
    }

    @Override
    public void cleanup() {
        LOG.info("Cleanup called on Selenium protocol drivers");
        synchronized (drivers) {
            drivers.forEach((d) -> {
                d.close();
            });
        }
    }
}