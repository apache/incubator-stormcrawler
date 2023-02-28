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
import java.net.URL;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.storm.Config;
import org.openqa.selenium.WebDriver.Timeouts;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;

/**
 * Delegates the requests to one or more remote selenium servers. The processes must be started /
 * stopped separately. The URLs to connect to are specified with the config 'selenium.addresses'.
 */
public class RemoteDriverProtocol extends SeleniumProtocol {

    @Override
    public void configure(Config conf) {
        super.configure(conf);

        // see https://github.com/SeleniumHQ/selenium/wiki/DesiredCapabilities
        DesiredCapabilities capabilities = new DesiredCapabilities();
        capabilities.setJavascriptEnabled(true);

        String userAgentString = getAgentString(conf);

        // custom capabilities
        Map<String, Object> confCapabilities =
                (Map<String, Object>) conf.get("selenium.capabilities");
        if (confCapabilities != null) {
            Iterator<Entry<String, Object>> iter = confCapabilities.entrySet().iterator();
            while (iter.hasNext()) {
                Entry<String, Object> entry = iter.next();
                Object val = entry.getValue();
                // substitute variable $useragent for the real value
                if (val instanceof String && "$useragent".equalsIgnoreCase(val.toString())) {
                    val = userAgentString;
                }
                capabilities.setCapability(entry.getKey(), val);
            }
        }

        // load addresses from config
        List<String> addresses = ConfUtils.loadListFromConf("selenium.addresses", conf);
        if (addresses.size() == 0) {
            throw new RuntimeException("No value found for selenium.addresses");
        }

        for (String cdaddress : addresses) {
            try {
                RemoteWebDriver driver = new RemoteWebDriver(new URL(cdaddress), capabilities);
                Timeouts touts = driver.manage().timeouts();
                int implicitWait = ConfUtils.getInt(conf, "selenium.implicitlyWait", 0);
                int pageLoadTimeout = ConfUtils.getInt(conf, "selenium.pageLoadTimeout", 0);
                int scriptTimeout = ConfUtils.getInt(conf, "selenium.scriptTimeout", 0);
                touts.implicitlyWait(Duration.ofMillis(implicitWait));
                touts.pageLoadTimeout(Duration.ofMillis(pageLoadTimeout));
                touts.scriptTimeout(Duration.ofMillis(scriptTimeout));
                drivers.add(driver);
            } catch (Exception e) {
                LOG.error(e.getLocalizedMessage(), e);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        RemoteDriverProtocol.main(new RemoteDriverProtocol(), args);
    }
}
