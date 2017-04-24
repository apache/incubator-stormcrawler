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
import java.util.List;

import org.apache.storm.Config;
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

        // TODO set capabilities via config
        // e.g. timeout? disable redirects? etc...

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
