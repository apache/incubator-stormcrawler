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

import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.openqa.selenium.WebDriver.Timeouts;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;

import com.digitalpebble.stormcrawler.util.ConfUtils;

/**
 * Delegates the requests to one or more remote selenium servers. The processes
 * must be started / stopped separately. The URLs to connect to are specified
 * with the config 'selenium.addresses'.
 **/

public class RemoteDriverProtocol extends SeleniumProtocol {

    @Override
    public void configure(Config conf) {
        super.configure(conf);

        DesiredCapabilities capabilities = new DesiredCapabilities();
        capabilities.setJavascriptEnabled(true);

        String userAgentString = getAgentString(conf);
        capabilities.setBrowserName(userAgentString);

        // custom capabilities
        Map<String, Object> confCapabilities = (Map<String, Object>) conf
                .get("selenium.capabilities");
        if (confCapabilities != null) {
            Iterator<Entry<String, Object>> iter = confCapabilities.entrySet()
                    .iterator();
            while (iter.hasNext()) {
                Entry<String, Object> entry = iter.next();
                capabilities.setCapability(entry.getKey(), entry.getValue());
            }
        }

        int timeout = ConfUtils.getInt(conf, "http.timeout", -1);

        // load adresses from config
        List<String> addresses = ConfUtils.loadListFromConf(
                "selenium.addresses", conf);
        if (addresses.size() == 0) {
            throw new RuntimeException("No value found for selenium.addresses");
        }
        try {
            for (String cdaddress : addresses) {
                RemoteWebDriver driver = new RemoteWebDriver(
                        new URL(cdaddress), capabilities);
                Timeouts touts = driver.manage().timeouts();
                touts.implicitlyWait(timeout, TimeUnit.MILLISECONDS);
                touts.pageLoadTimeout(timeout, TimeUnit.MILLISECONDS);
                touts.setScriptTimeout(timeout, TimeUnit.MILLISECONDS);
                drivers.add(driver);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        RemoteDriverProtocol.main(new RemoteDriverProtocol(), args);
    }

}
