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
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.storm.Config;
import org.openqa.selenium.Dimension;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;

public class SeleniumGridImpl extends SeleniumGridProtocol {

    private TimerTask timerTask;
    private Timer updateQueue;
    private int noOfWorkers = 0;
    private String gridAddress;
    private final DesiredCapabilities capabilities = new DesiredCapabilities();

    @Override
    public void configure(Config conf) {
        super.configure(conf);
        noOfWorkers = ConfUtils.getInt(conf, "topology.workers", 2);
        gridAddress = super.gridAddress;
        capabilities.setBrowserName(
                ConfUtils.getString(conf, "selenium.capabilities.browserName", "chrome"));
        capabilities.setCapability("newSessionWaitTimeout", 600);
        capabilities.setCapability("browserTimeout", 600);
        updateQueue = new Timer();
        updateQueueOfBrowsers();
    }

    protected void updateQueueOfBrowsers() {
        timerTask =
                new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            List list = getAllNodesList();
                            LOG.info("Blocking Queue size: " + driversQueue.size());
                            while (getSessionsCount(list) > 0) {
                                int size = list.size();
                                // Check if the queue size is more than the actual
                                // no of browsers allowed per worker
                                // means crawler services are idle because all drivers are in queue
                                if (driversQueue.size() >= (size / noOfWorkers)) {
                                    SeleniumGridProtocol.Holder holder = driversQueue.take();
                                    long totalTime = System.currentTimeMillis() - holder.getTime();
                                    // clear the queue if the total time spent in the queue is more
                                    // than 4.5 minutes
                                    // As after 5 mintues the driver would be idle and will throw
                                    // exception
                                    // while we try to use that driver for fetching
                                    if (totalTime > 1000 * 4.5 * 60) {
                                        driversQueue.clear();
                                    }
                                }
                                // so that browsers get equally divided among workers
                                if (driversQueue.size() <= size / noOfWorkers) {
                                    RemoteWebDriver driver = getDriverFromNode();
                                    if (driver != null) {
                                        driversQueue.put(
                                                new SeleniumGridProtocol.Holder(
                                                        driver, System.currentTimeMillis()));
                                        LOG.info(
                                                "Placed driver in blocking queue: "
                                                        + driversQueue.size());
                                    }
                                }
                                list = getAllNodesList();
                            }
                        } catch (Exception e) {
                            LOG.error(
                                    "Exception while running task for adding driver to the queue",
                                    e);
                        }
                    }
                };
        // update the queue every 5 minutes
        updateQueue.schedule(timerTask, 0 * 60 * 1000, 5 * 60 * 1000);
    }

    protected RemoteWebDriver getDriverFromNode() {
        int sessionCount = 0;
        RemoteWebDriver driver = null;

        try {
            LOG.debug("Adding new driver from " + gridAddress);
            driver = new RemoteWebDriver(new URL(gridAddress), capabilities);
            WebDriver.Timeouts touts = driver.manage().timeouts();
            WebDriver.Window window = driver.manage().window();
            touts.implicitlyWait(Duration.ofSeconds(6));
            touts.pageLoadTimeout(Duration.ofSeconds(60));
            touts.scriptTimeout(Duration.ofSeconds(30));
            window.setSize(new Dimension(1980, 1280));
            LOG.debug("Inside getDriverFromGrid to set web drivers" + driver.hashCode());
        } catch (Exception e) {
        }
        return driver;
    }
}
