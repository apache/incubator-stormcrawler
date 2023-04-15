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
import java.util.List;
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

    private HashMap<String, Object> config;

    @Rule public Timeout globalTimeout = Timeout.seconds(60);

    @BeforeClass
    public static void beforeClass() {
        executorService = Executors.newFixedThreadPool(4);
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

        config = new HashMap<String, Object>();
        config.put(
                "urlbuffer.class",
                "com.digitalpebble.stormcrawler.persistence.urlbuffer.SimpleURLBuffer");
        config.put(Constants.URLFRONTIER_HOST_KEY, connection.getHost());
        config.put(Constants.URLFRONTIER_PORT_KEY, connection.getPort());
        config.put(
                "scheduler.class", "com.digitalpebble.stormcrawler.persistence.DefaultScheduler");
        config.put("status.updater.cache.spec", "maximumSize=10000,expireAfterAccess=1h");
        config.put("metadata.persist", persistedKey);
        config.put("urlfrontier.connection.checker.interval", 2);
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
        final long start = System.currentTimeMillis();
        Future<Boolean> future = executorService.submit(
                () -> {
                    List<Tuple> outputList = output.getAckedTuples();
                    while (outputList.stream()
                            .filter((tuple) -> tuple.getStringByField("url").equals(url))
                            .count() == 0) {
                        Thread.sleep(100);
                        // Additional if-clause for checking timeout necessary, otherwise the
                        // thread would unnecessarily keep running
                        if (System.currentTimeMillis() - start > timeoutSeconds * 1000) {
                            return false;
                        }
                        outputList = output.getAckedTuples();
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
        String url = "http://example.com/?test=1";
        Metadata metadata = new Metadata();
        metadata.setValue(persistedKey, "somePersistedMetaInfo");
        metadata.setValue(notPersistedKey, "someNotPersistedMetaInfo");

        store(url, Status.DISCOVERED, metadata);
        Assert.assertEquals(true, isAcked(url, 1));
    }

    @Test
    public void worksAfterFrontierRestart()
            throws ExecutionException, InterruptedException, TimeoutException {
        String url = "http://example.com/?test=1";
        store(url, Status.DISCOVERED, new Metadata());
        Assert.assertEquals(true, isAcked(url, 1));

        urlFrontierContainer.stop();

        url = "http://example.com/?test=2";
        store(url, Status.DISCOVERED, new Metadata());
        Assert.assertEquals(false, isAcked(url, 1));

        urlFrontierContainer.start();

        // Give the connection checker the time to re-establish the connection to the frontier
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        url = "http://example.com/?test=3";
        store(url, Status.DISCOVERED, new Metadata());
        Assert.assertEquals(true, isAcked(url, 1));
    }

    @Test
    public void exceedingMaxMessagesInFlight()
            throws ExecutionException, InterruptedException, TimeoutException {
        // Sending two URLs and therefore exceeding the maximum number of messages in flight
        // This must not lead to starvation after frontier restart.
        store("http://example.com/?test=1", Status.DISCOVERED, new Metadata());
        store("http://example.com/?test=2", Status.DISCOVERED, new Metadata());
        Assert.assertEquals(true, isAcked("http://example.com/?test=1", 1));
        Assert.assertEquals(true, isAcked("http://example.com/?test=2", 1));

        urlFrontierContainer.stop();

        store("http://example.com/?test=3", Status.DISCOVERED, new Metadata());
        store("http://example.com/?test=4", Status.DISCOVERED, new Metadata());
        Assert.assertEquals(false, isAcked("http://example.com/?test=3", 1));
        Assert.assertEquals(false, isAcked("http://example.com/?test=4", 1));

        urlFrontierContainer.start();

        // Give the connection checker the time to re-establish the connection to the frontier
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        store("http://example.com/?test=5", Status.DISCOVERED, new Metadata());
        store("http://example.com/?test=6", Status.DISCOVERED, new Metadata());
        Assert.assertEquals(true, isAcked("http://example.com/?test=5", 1));
        Assert.assertEquals(true, isAcked("http://example.com/?test=6", 1));
    }
}
