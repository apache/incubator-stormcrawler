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
package com.digitalpebble.stormcrawler.urlfrontier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.TestOutputCollector;
import com.digitalpebble.stormcrawler.TestUtil;
import com.digitalpebble.stormcrawler.persistence.Status;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class StatusUpdaterBoltTest {
    private StatusUpdaterBolt bolt;
    private TestOutputCollector output;
    private URLFrontierContainer urlFrontierContainer;

    private static final String persistedKey = "somePersistedKey";
    private static final String notPersistedKey = "someNotPersistedKey";

    private static ExecutorService executorService;

    @Rule public Timeout globalTimeout = Timeout.seconds(60);

    @BeforeClass
    public static void beforeClass() {
        executorService = Executors.newFixedThreadPool(2);
    }

    @AfterClass
    public static void afterClass() {
        executorService.shutdown();
        executorService = null;
    }

    @Before
    public void before() {

        String image = "crawlercommons/url-frontier";

        String version = System.getProperty("urlfrontier-version");
        if (version != null) image += ":" + version;

        urlFrontierContainer = new URLFrontierContainer(image);
        urlFrontierContainer.start();

        bolt = new StatusUpdaterBolt();

        var connection = urlFrontierContainer.getFrontierConnection();

        final var config = new HashMap<String, Object>();
        config.put(
                "urlbuffer.class",
                "com.digitalpebble.stormcrawler.persistence.urlbuffer.SimpleURLBuffer");
        config.put(Constants.URLFRONTIER_HOST_KEY, connection.getHost());
        config.put(Constants.URLFRONTIER_PORT_KEY, connection.getPort());
        config.put(
                "scheduler.class", "com.digitalpebble.stormcrawler.persistence.DefaultScheduler");
        config.put("status.updater.cache.spec", "maximumSize=10000,expireAfterAccess=1h");
        config.put("metadata.persist", persistedKey);
        config.put("urlfrontier.updater.max.messages", 1);
        config.put("urlfrontier.cache.expireafter.sec", 10);

        output = new TestOutputCollector();
        bolt.prepare(config, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
    }

    @After
    public void after() {
        bolt.cleanup();
        urlFrontierContainer.close();
        output = null;
    }

    private void store(String url, Status status, Metadata metadata) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getValueByField("status")).thenReturn(status);
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);

        bolt.execute(tuple);
    }

    private boolean isAcked(String url, long timeoutSeconds) {
        return isAcked(url, timeoutSeconds, System.currentTimeMillis());
    }

    private boolean isAcked(String url, long timeoutSeconds, long start) {
        Future<Boolean> future =
                executorService.submit(
                        () -> {
                            while (output.getAckedTuples().stream()
                                            .filter(
                                                    (tuple) ->
                                                            tuple.getStringByField("url")
                                                                    .equals(url))
                                            .count()
                                    == 0) {
                                Thread.sleep(100);
                                // Additional if-clause for checking timeout necessary, otherwise
                                // the thread would unnecessarily keep running
                                if (System.currentTimeMillis() - start > timeoutSeconds * 1000) {
                                    return false;
                                }
                            }
                            return true;
                        });

        try {
            return future.get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (Exception e) {
            return false;
        }
    }

    @Test
    public void canAckSimpleTupleWithMetadata()
            throws ExecutionException, InterruptedException, TimeoutException {
        final var url = "https://www.url.net/something";
        final var meta = new Metadata();
        meta.setValue(persistedKey, "somePersistedMetaInfo");
        meta.setValue(notPersistedKey, "someNotPersistedMetaInfo");

        store(url, Status.DISCOVERED, meta);
        Assert.assertEquals(true, isAcked(url, 5));
    }

    @Test
    public void exceedingMaxMessagesInFlightAfterFrontierRestart()
            throws ExecutionException, InterruptedException, TimeoutException {
        // Stopping the frontier to simulate the following situation:
        // The inFlightSemaphore runs full during an intermediate downtime of the frontier
        urlFrontierContainer.stop();

        // Sending two URLs and therefore exceeding the maximum number of messages in flight
        // This must not lead to starvation after frontier restart.
        store("http://example.com/?test=1", Status.DISCOVERED, new Metadata());
        store("http://example.com/?test=2", Status.DISCOVERED, new Metadata());
        long start = System.currentTimeMillis();
        Assert.assertEquals(false, isAcked("http://example.com/?test=1", 5, start));
        Assert.assertEquals(false, isAcked("http://example.com/?test=2", 5, start));

        urlFrontierContainer.start();

        store("http://example.com/?test=3", Status.DISCOVERED, new Metadata());
        Assert.assertEquals(true, isAcked("http://example.com/?test=3", 10));
    }
}
