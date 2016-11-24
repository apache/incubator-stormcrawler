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

import org.apache.storm.Config;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.protocol.AbstractHttpProtocol;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.machinepublishers.jbrowserdriver.JBrowserDriver;
import com.machinepublishers.jbrowserdriver.Settings;

import crawlercommons.robots.BaseRobotRules;

/**
 * Uses JBrowserdriver to handle http and https
 **/

public class HttpProtocol extends AbstractHttpProtocol {

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(HttpProtocol.class);

    private JBrowserDriver driver;

    private NavigationFilters filters;

    @Override
    public void configure(Config conf) {
        super.configure(conf);

        // TODO configure timeout + http agent +proxy etc...
        Settings settings = Settings.builder().headless(true).build();

        driver = new JBrowserDriver(settings);

        filters = NavigationFilters.fromConf(conf);
    }

    public ProtocolResponse getProtocolOutput(String url, Metadata metadata)
            throws Exception {
        // TODO is this really needed?
        synchronized (driver) {
            // This will block for the page load and any
            // associated AJAX requests
            driver.get(url);

            // call the filters
            ProtocolResponse response = filters.filter(driver, metadata);
            if (response != null)
                return response;

            // if no filters got triggered
            byte[] content = driver.getPageSource().getBytes();
            return new ProtocolResponse(content, driver.getStatusCode(),
                    metadata);
        }
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
        ProtocolResponse response = protocol.getProtocolOutput(url, md);
        System.out.println(url);
        System.out.println(response.getMetadata());
        System.out.println(response.getStatusCode());
        System.out.println(response.getContent().length);

        System.exit(0);
    }

}