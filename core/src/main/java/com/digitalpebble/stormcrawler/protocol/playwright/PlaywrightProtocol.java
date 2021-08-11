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
import com.microsoft.playwright.options.WaitUntilState;
import org.apache.commons.lang.mutable.MutableObject;
import org.apache.storm.Config;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PlaywrightProtocol extends AbstractHttpProtocol {
    protected static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(PlaywrightProtocol.class);

    final Map<String, Metadata> inputQueue = Collections
            .synchronizedMap(new LinkedHashMap<String, Metadata>());
    Map<String, BrowserResponse> outputQueue = Collections
            .synchronizedMap(new LinkedHashMap<String, BrowserResponse>());

    protected String userAgent;
    protected int screenWidth;
    protected int screenHeight;
    protected int timeout;
    protected WaitUntilState loadEvent;
    protected int maxContent;
    protected boolean insecure;
    protected boolean recoverTimeouts;

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
        recoverTimeouts = ConfUtils.getBoolean(conf, "playwright.timeout.recover", true);
        String loadEventString = ConfUtils.getString(conf, "playwright.load_event", "load");
        maxContent = ConfUtils.getInt(conf, "playwright.content.limit", -1);
        insecure = ConfUtils.getBoolean(conf, "playwright.insecure", false);
        boolean headless = ConfUtils.getBoolean(conf, "playwright.headless", true);

        // load amount of concurrent instances that can be run per worker
        int numInst = ConfUtils.getInt(conf, "playwright.instances.num", 1);

        LOG.info("launching playwright protocol with {} threads", numInst);

        // map load event string to playwright native wait stat
        switch (loadEventString) {
            case "domcontentloaded":
                loadEvent = WaitUntilState.DOMCONTENTLOADED;
                break;
            case "networkidle":
                loadEvent = WaitUntilState.NETWORKIDLE;
                break;
            default:
                loadEvent = WaitUntilState.LOAD;
                break;
        }

        // synchronize across JVM to prevent duplicate threads
        synchronized (PlaywrightProtocol.class) {
            // iterate over total amount of instances requested creating a new thread  for each
            for (int i = 0; i < numInst; i++) {
                // create a new thread for a browser instance
                BrowserThread thread = new BrowserThread(userAgent, screenWidth, screenHeight, timeout, loadEvent,
                        maxContent, insecure, headless, browserName);
                // set daemon as true because we are lazy and don't want to track closure (we should probably change this)
                thread.setDaemon(true);
                // launch thread start
                thread.start();
            }
        }
    }

    @Override
    public ProtocolResponse getProtocolOutput(String url, Metadata metadata) throws Exception {
        // add url pair to the input queue to begin execution process
        inputQueue.put(url, metadata);

        // wait until url processing has completed
        while (!outputQueue.containsKey(url)) { Thread.sleep(5); }

        // load response from output queue
        BrowserResponse response = outputQueue.get(url);

        // handle exception
        if (!response.ok())
            throw response.getException();

        return response.getResponse();
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

    public static void main(String args[]) throws Exception {
        PlaywrightProtocol.main(new PlaywrightProtocol(), args);
    }

    private class BrowserResponse {
        protected final ProtocolResponse response;
        protected final Exception exception;

        private BrowserResponse(ProtocolResponse response, Exception exception) {
            this.response = response;
            this.exception = exception;
        }

        private boolean ok() { return exception == null; }

        private ProtocolResponse getResponse() { return response; }

        private Exception getException() { return exception; }
    }

    private class BrowserThread extends Thread {
        protected final String userAgent;
        protected final int screenWidth;
        protected final int screenHeight;
        protected final int timeout;
        protected final WaitUntilState loadEvent;
        protected final int maxContent;
        protected final boolean insecure;

        protected final Browser browser;

        private BrowserThread(String userAgent, int screenWidth, int screenHeight, int timeout, WaitUntilState loadEvent,
                              int maxContent, boolean insecure, boolean headless, String browserName) {
            this.userAgent = userAgent;
            this.screenWidth = screenWidth;
            this.screenHeight = screenHeight;
            this.timeout = timeout;
            this.loadEvent = loadEvent;
            this.maxContent = maxContent;
            this.insecure = insecure;

            // create default launch parameters
            BrowserType.LaunchOptions launchOpts = new BrowserType.LaunchOptions();

            // set headless mode
            launchOpts.setHeadless(headless);

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

            browser = browserType.launch(launchOpts);
        }

        @Override
        public void run() {
            while (true) {
                // create variables to hold input values
                String url = null;
                Metadata metadata = null;

                // safely load the input values from the input queue
                synchronized (inputQueue) {
                    // iterate over the input queue map attempting to load the first available pair
                    for (Map.Entry<String, Metadata> entry : inputQueue.entrySet()) {
                        // retrieve the url as the key from the entry
                        url = entry.getKey();
                        // retrieve the meta as the value from the entry
                        metadata = entry.getValue();
                        // break loop on successful load of input
                        break;
                    }

                    // conditionally remove url from input queue
                    if (url != null)
                        inputQueue.remove(url);
                }

                // ensure that all necessary data was loaded
                if (url == null || metadata == null) {
                    try {
                        // wait 50ms before querying queue again to prevent unnecessary resource usage during empty queues
                        Thread.sleep(50);
                        continue;
                    } catch (InterruptedException e) {
                        LOG.error("{} caught interrupted exception", getName());
                        Thread.currentThread().interrupt();
                    }
                }

                LOG.debug("fetching {} with playwright", url);

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

                LOG.info("creating playwright context: {}", url);

                // wrap in generic try/catch as playwright operates over an RPC layer meaning we could have an unexpected error
                try {
                    // create a new browser context
                    context = browser.newContext(ctxOpts);
                } catch (Exception e) {
                    // send null back to the calling thread to indicate we had an internal exception
                    outputQueue.put(url, new BrowserResponse(null, new RuntimeException("failed to open new playwright context", e)));
                    // begin next url pair
                    continue;
                }

                LOG.info("configuring playwright context timeout: {}", url);

                // set default timeout for all context actions
                context.setDefaultTimeout(timeout);
                // set default timeout for all context navigations
                context.setDefaultNavigationTimeout(timeout);

                LOG.info("creating playwright page: {}", url);

                // create variable to hold new page for fetch operation
                Page page;

                // wrap in generic try/catch as playwright operates over an RPC layer meaning we could have an unexpected error
                try {
                    // open a new page for the load operation
                    page = context.newPage();
                } catch (Exception e) {
                    LOG.error("failed to open new playwright page", e);
                    // send null back to the calling thread to indicate we had an internal exception
                    outputQueue.put(url, new BrowserResponse(null, new RuntimeException("failed to open new playwright page", e)));
                    // begin next url pair
                    continue;
                }

                // ensure that unhandled error are logged but do not cause failures
                String finalUrl = url;
                page.onPageError(exception -> {
                    LOG.warn("playwright internal page error {}: {}", finalUrl, exception);
                });

                LOG.info("navigating playwright page: {}", url);

                // create variable to hold response from playwright browser
                Response response = null;

                // navigate to the desired url
                try {
                    response = page.navigate(url, new Page.NavigateOptions().setWaitUntil(loadEvent));
                } catch (TimeoutError e) {
                    LOG.error("playwright timeout calling {}", url, e);

                    // conditionally fail operation on timeout
                    if (!recoverTimeouts || page.content().length() == 0) {
                        // close page explicitly
                        page.close();
                        // close context
                        context.close();
                        // send null back to the calling thread to indicate we had an internal exception
                        outputQueue.put(url, new BrowserResponse(null, new RuntimeException("playwright timeout calling " + url)));
                        // begin next url pair
                        continue;
                    }
                } catch (Exception e) {
                    // close page explicitly
                    page.close();
                    // close context
                    context.close();
                    LOG.error("playwright exception calling {}", url, e);
                    // send null back to the calling thread to indicate we had an internal exception
                    outputQueue.put(url, new BrowserResponse(null, new RuntimeException("playwright exception calling " + url, e)));
                    // begin next url pair
                    continue;
                }

                LOG.info("loading content from playwright page: {}", url);

                // retrieve content from page
                String content = page.content();

                // create a new metadata object to fill with response data
                Metadata responseMetadata = new Metadata();

                // create variable to hold response status and default to 200
                int status = 200;

                // conditionally load header from response object
                if (response != null) {
                    LOG.info("loading header from playwright response: {}", url);
                    Map<String, String> headers = response.headers();
                    status = response.status();

                    // iterate over response headers loading the permitted headers into the metadata object
                    for (Map.Entry<String, String> entry : headers.entrySet()) {
                        // retrieve key and value for header
                        String key = entry.getKey();
                        String value = entry.getValue();

                        // decode headers matching those that are expected to be in base64 format
                        if (key.equals(ProtocolResponse.REQUEST_HEADERS_KEY)
                                || key.equals(ProtocolResponse.RESPONSE_HEADERS_KEY)) {
                            value = new String(Base64.getDecoder().decode(value));
                        }

                        // add header to metadata for response
                        responseMetadata.addValue(key.toLowerCase(Locale.ROOT), value);
                    }
                }

                LOG.info("closing playwright page: {}", url);

                // close page
                page.close();

                LOG.info("closing playwright context: {}", url);

                // close context
                context.close();

                LOG.info("playwright access completed: {}", url);

                // create a new mutable object to track the trim status of the response body
                MutableObject trimmed = new MutableObject(
                        ProtocolResponse.TrimmedContentReason.NOT_TRIMMED);

                // load response body into byte array using helper function that tracks trimming changes
                byte[] bytes = toByteArray(content, trimmed);

                // conditionally mark content as trimmed in metadata and logs
                if (trimmed.getValue() != ProtocolResponse.TrimmedContentReason.NOT_TRIMMED) {
                    responseMetadata.setValue(ProtocolResponse.TRIMMED_RESPONSE_KEY,
                            "true");
                    responseMetadata.setValue(
                            ProtocolResponse.TRIMMED_RESPONSE_REASON_KEY,
                            trimmed.getValue().toString().toLowerCase(Locale.ROOT));
                    LOG.warn("HTTP playwright content trimmed to {}", bytes.length);
                }

                LOG.debug("returning response from playwright protocol: {}", url);

                // send result or url data back to calling thread
                outputQueue.put(
                        url, new BrowserResponse(
                                new ProtocolResponse(bytes, status, responseMetadata), null
                        )
                );
            }
        }
    }
}