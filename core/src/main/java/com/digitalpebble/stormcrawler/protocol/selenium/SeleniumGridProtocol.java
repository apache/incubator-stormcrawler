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

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.protocol.AbstractHttpProtocol;
import com.digitalpebble.stormcrawler.protocol.HttpHeaders;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.google.gson.Gson;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.storm.Config;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.slf4j.LoggerFactory;

public abstract class SeleniumGridProtocol extends AbstractHttpProtocol {
    protected static final org.slf4j.Logger LOG =
            LoggerFactory.getLogger(SeleniumGridProtocol.class);
    protected static LinkedBlockingQueue<Holder> driversQueue;
    private NavigationFilters filters;
    private final DesiredCapabilities capabilities = new DesiredCapabilities();

    protected String gridAddress;

    @Override
    public void configure(Config conf) {
        super.configure(conf);
        driversQueue = new LinkedBlockingQueue<>();
        filters = NavigationFilters.fromConf(conf);
        gridAddress =
                ConfUtils.getString(conf, "selenium.grid.address", "http://localhost:4444/wd/hub");
    }

    protected synchronized List<Map<String, Object>> getAllNodesList() throws IOException {
        Map<String, Object> valueMap = null;
        boolean ready = false;
        while (!ready) {
            Map<String, Object> map = getStatusStream();
            valueMap = (Map<String, Object>) map.get("value");
            ready = (boolean) valueMap.get("ready");
            if (!ready) {
                LOG.warn("Selenium Grid is not ready yet");
            }
        }
        LOG.info("Grid Is Ready to Serve");
        return (List<Map<String, Object>>) valueMap.get("nodes");
    }

    private Map<String, Object> getStatusStream() throws IOException {
        Gson gson = new Gson();
        URL url = new URL(gridAddress + "/status");
        InputStream stream = url.openStream();
        Reader reader = new InputStreamReader(stream);
        Map<String, Object> map = gson.fromJson(reader, Map.class);
        stream.close();
        return map;
    }

    protected int getSessionsCount(List<Map<String, Object>> nodes) {
        int availableSessions = 0;
        for (Map<String, Object> node : nodes) {
            List<Map<String, Object>> slots = (List<Map<String, Object>>) node.get("slots");
            for (Map<String, Object> slot : slots) {
                if (slot.get("session") == null) {
                    availableSessions++;
                }
            }
        }
        return availableSessions;
    }

    public class Holder {
        public RemoteWebDriver driver;
        public Long time;

        public void setDriver(RemoteWebDriver driver) {
            this.driver = driver;
        }

        public void setTime(Long time) {
            this.time = time;
        }

        public RemoteWebDriver getDriver() {
            return this.driver;
        }

        public Long getTime() {
            return this.time;
        }

        public Holder(RemoteWebDriver driver, Long time) {
            this.driver = driver;
            this.time = time;
        }
    }

    public ProtocolResponse getProtocolOutput(String url, Metadata metadata) throws Exception {
        RemoteWebDriver driver;
        while ((driver = getDriver()) == null) {}
        try {
            // This will block for the page load and any
            // associated AJAX requests
            driver.get(url);

            String u = driver.getCurrentUrl();

            // call the filters
            ProtocolResponse response = filters.filter(driver, metadata);
            if (response != null) {
                return response;
            }

            // if the URL is different then we must have hit a redirection
            if (!u.equalsIgnoreCase(url)) {
                byte[] content = new byte[] {};
                Metadata m = new Metadata();
                m.addValue(HttpHeaders.LOCATION, u);
                return new ProtocolResponse(content, 307, m);
            }

            // if no filters got triggered
            byte[] content = driver.getPageSource().getBytes();
            return new ProtocolResponse(content, 200, new Metadata());

        } catch (Exception e) {
            if (e.getMessage() != null) {
                if ((e.getMessage().contains("ERR_NAME_NOT_RESOLVED")
                        || e.getMessage().contains("ERR_CONNECTION_REFUSED")
                        || e.getMessage().contains("ERR_CONNECTION_CLOSED")
                        || e.getMessage().contains("ERR_SSL_PROTOCOL_ERROR")
                        || e.getMessage().contains("ERR_CONNECTION_RESET")
                        || e.getMessage().contains("ERR_SSL_VERSION_OR_CIPHER_MISMATCH")
                        || e.getMessage().contains("ERR_ADDRESS_UNREACHABLE"))) {
                    LOG.info(
                            "Exception is of webpage related hence continuing with the driver and adding it back to queue");
                } else {
                    LOG.error(
                            "Exception wile doing operation via driver url {} with driver hashcode {}"
                                    + "with excepiton {}",
                            url,
                            driver.hashCode(),
                            e);
                    closeConnectionGracefully(driver);
                    driver = null;
                }
            }
            throw new Exception(e);
        } finally {
            // finished with this driver - return it to the queue
            if (driver != null) driversQueue.put(new Holder(driver, System.currentTimeMillis()));
        }
    }

    private final RemoteWebDriver getDriver() {
        try {
            return driversQueue.take().getDriver();
        } catch (Exception e) {
            return null;
        }
    }

    private void closeConnectionGracefully(RemoteWebDriver driver) {
        try {
            LOG.info("Before disposing driver : {}", driver.hashCode());

            if (driver.getSessionId() != null) {
                if (driver.getSessionId().toString() != null) driver.quit();
            }
        } catch (Exception e) {
            LOG.info("Error while closing driver", e);
        }
    }
}
