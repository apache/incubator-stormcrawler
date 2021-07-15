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

package com.digitalpebble.stormcrawler.protocol.playwright;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.protocol.AbstractHttpProtocol;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import com.digitalpebble.stormcrawler.proxy.SCProxy;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.microsoft.playwright.*;
import com.microsoft.playwright.options.Proxy;
import org.apache.commons.lang.mutable.MutableObject;
import org.apache.storm.Config;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PlaywrightProtocol extends AbstractHttpProtocol {
    private class ThreadSafeBrowser {
        protected final Lock lock;
        protected final Browser browser;

        private ThreadSafeBrowser(Browser browser) {
            ReentrantReadWriteLock l = new ReentrantReadWriteLock();
            lock = l.writeLock();
            this.browser = browser;
        }

        private void lockBrowser() {
            lock.lock();
        }

        private void unlockBrowser() {
            lock.unlock();
        }

        private Browser getBrowser() {
            return browser;
        }
    }

    protected static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(PlaywrightProtocol.class);

    protected String userAgent;
    protected int screenWidth;
    protected int screenHeight;
    protected int timeout;
    protected int maxContent;
    protected boolean insecure;

    protected LinkedBlockingQueue<ThreadSafeBrowser> drivers;

    @Override
    public void configure(Config conf) {
        // call abstract class configure function to setup default values
        super.configure(conf);

        // load browser name to mark what browser will be used (chrome, firefox, webkit)
        String browserName = ConfUtils.getString(conf, "playwright.browser.name", "chrome");

        // load configurations
        userAgent = ConfUtils.getString(conf, "playwright.agent.name", "chrome");
        screenWidth = ConfUtils.getInt(conf, "playwright.browser.width", 1920);
        screenHeight = ConfUtils.getInt(conf, "playwright.browser.height", 1080);
        timeout = ConfUtils.getInt(conf, "playwright.timeout", 10000);
        maxContent = ConfUtils.getInt(conf, "playwright.content.limit", -1);
        insecure = ConfUtils.getBoolean(conf, "playwright.insecure", false);
        boolean headless = ConfUtils.getBoolean(conf, "playwright.headless", true);

        // load amount of concurrent instances that can be run per worker
        int numInst = ConfUtils.getInt(conf, "playwright.instances.num", 1);

        // create drivers list
        drivers = new LinkedBlockingQueue<>();

        // create default launch parameters
        BrowserType.LaunchOptions launchOpts = new BrowserType.LaunchOptions();

        // set headless mode
        launchOpts.setHeadless(headless);

        // only permit class per JVM
        synchronized (PlaywrightProtocol.class) {
            // iterate over total amount of instances requested creating a new browser for each
            for (int i = 0; i < numInst; i++) {
                // initialize a playwright RPC connection
                Playwright playwright = Playwright.create();

                // create variable to hold playwright browser type that will be used
                BrowserType browserType;

                // map the passed browser name to the playwright browser type
                switch (browserName) {
                    case "firefox":
                    case "mozilla":
                    case "gecko":
                        LOG.info("initializing playwright protocol with firefox browser");
                        browserType = playwright.firefox();
                        break;
                    case "safari":
                    case "webkit":
                    case "apple":
                        LOG.info("initializing playwright protocol with webkit browser");
                        browserType = playwright.webkit();
                        break;
                    default:
                        LOG.info("initializing playwright protocol with chromium browser");
                        browserType = playwright.chromium();
                        break;
                }

                // launch new browser, wrap browser in thread safe class wrapper, and add to drivers
                drivers.add(new ThreadSafeBrowser(browserType.launch(launchOpts)));
            }
        }
    }

    @Override
    public ProtocolResponse getProtocolOutput(String url, Metadata metadata) {
        // retrieve next available driver
        ThreadSafeBrowser tsBrowser = getDriver();

        // ensure that browser is not null
        if (tsBrowser == null)
            throw new RuntimeException("browser returned from playwright protocol was null");

        // lock browser execution
        tsBrowser.lockBrowser();

        // retrieve internal browser
        Browser browser = tsBrowser.getBrowser();

        // create default configuration for the browser context
        Browser.NewContextOptions ctxOpts = new Browser.NewContextOptions();

        // set security configurations
        ctxOpts.setAcceptDownloads(false);
        ctxOpts.setIgnoreHTTPSErrors(insecure);

        // conditionally set custom user agent
        if (userAgent != null)
            ctxOpts.setUserAgent(userAgent);

        // set resolution
        ctxOpts.setScreenSize(screenWidth, screenHeight);

        // conditionally add a dynamic proxy
        if (proxyManager != null) {
            System.out.println("adding proxy: " + proxyManager.getClass().getName());
            // retrieve proxy from proxy manager
            SCProxy prox = proxyManager.getProxy(metadata);

            // create proxy from native SC proxy class
            Proxy proxy = new Proxy(prox.toString());

            // conditionally add authentication to proxy
            if (!prox.getUsername().isEmpty()) {
                proxy.setUsername(prox.getUsername());
                proxy.setPassword(prox.getPassword());
            }

            // set proxy for browser context
            ctxOpts.setProxy(proxy);
        }

        // create variable to hold browser context
        BrowserContext context;

        try {
            // create a new browser context
            context = browser.newContext(ctxOpts);
        } catch (Exception e) {
            tsBrowser.unlockBrowser();
            throw e;
        }

        // open a new page for the load operation
        Page page = context.newPage();

        // create variable to hold response from playwright browser
        Response response;

        // navigate to the desired url
        try {
            response = page.navigate(url, new Page.NavigateOptions().setTimeout(timeout));
        } catch (TimeoutError ignored) {
            tsBrowser.unlockBrowser();
            LOG.error("playwright timeout calling: {}", url);
            return null;
        } catch (Exception e) {
            tsBrowser.unlockBrowser();
            throw e;
        }

        // retrieve content from page
        String content = page.content();

        // close page
        page.close();

        // release browser as we have concluded our unsafe operations
        tsBrowser.unlockBrowser();

        // create a new metadata object to fill with response data
        Metadata responsemetadata = new Metadata();

        // load header from response object
        Map<String, String> headers = response.headers();

        // iterate over response headers loading the permitted headers into the metadata object
        for (Map.Entry<String,String> entry : headers.entrySet()) {
            // retrieve key and value for header
            String key = entry.getKey();
            String value = entry.getValue();

            // decode headers matching those that are expected to be in base64 format
            if (key.equals(ProtocolResponse.REQUEST_HEADERS_KEY)
                    || key.equals(ProtocolResponse.RESPONSE_HEADERS_KEY)) {
                value = new String(Base64.getDecoder().decode(value));
            }

            // add header to metadata for response
            responsemetadata.addValue(key.toLowerCase(Locale.ROOT), value);
        }

        // create a new mutable object to track the trim status of the response body
        MutableObject trimmed = new MutableObject(
                ProtocolResponse.TrimmedContentReason.NOT_TRIMMED);

        // load response body into byte array using helper function that tracks trimming changes
        byte[] bytes = toByteArray(content, trimmed);

        // conditionally mark content as trimmed in metadata and logs
        if (trimmed.getValue() != ProtocolResponse.TrimmedContentReason.NOT_TRIMMED) {
            responsemetadata.setValue(ProtocolResponse.TRIMMED_RESPONSE_KEY,
                    "true");
            responsemetadata.setValue(
                    ProtocolResponse.TRIMMED_RESPONSE_REASON_KEY,
                    trimmed.getValue().toString().toLowerCase(Locale.ROOT));
            LOG.warn("HTTP playwright content trimmed to {}", bytes.length);
        }

        return new ProtocolResponse(bytes, response.status(),
                responsemetadata);
    }

    private byte[] toByteArray(final String responseBody,
                               MutableObject trimmed) {
        // return empty body for null response
        if (responseBody == null)
            return new byte[] {};

        // load bytes from HTML content
        byte[] arr = responseBody.getBytes();

        // conditionally trim content
        if (maxContent != -1) {
            // set max content by the smaller value between the max int size and the max content length
            int maxContentBytes = Math.min(Integer.MAX_VALUE, maxContent);
            // slice byte array to trimmed content size
            arr = Arrays.copyOfRange(arr, 0, maxContentBytes);
            // mark metadata as being trimmed
            trimmed.setValue(ProtocolResponse.TrimmedContentReason.LENGTH);
        }

        return arr;
    }


    /** Returns the first available driver **/
    private ThreadSafeBrowser getDriver() {
        try {
            return drivers.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return null;
    }
}
