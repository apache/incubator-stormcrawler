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

        output = new TestOutputCollector();
        bolt.prepare(config, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
    }

    @After
    public void after() {
        bolt.cleanup();
        urlFrontierContainer.close();
        output = null;
    }

    @SuppressWarnings("SameParameterValue")
    private Future<List<Tuple>> store(String url, Status status, Metadata metadata) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getValueByField("status")).thenReturn(status);
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);

        int numberOfAckedTuples = output.getAckedTuples().size();
        bolt.execute(tuple);

        return executorService.submit(
                () -> {
                    var outputList = output.getAckedTuples();
                    while (outputList.size() == numberOfAckedTuples) {
                        Thread.sleep(100);
                        outputList = output.getAckedTuples();
                    }
                    return outputList;
                });
    }

    @Test
    public void canAckASimpleTuple()
            throws ExecutionException, InterruptedException, TimeoutException {
        final var url = "https://www.url.net/something";
        final var meta = new Metadata();
        meta.setValue(persistedKey, "somePersistedMetaInfo");
        meta.setValue(notPersistedKey, "someNotPersistedMetaInfo");

        var future = store(url, Status.DISCOVERED, meta);
        List<Tuple> ackedTuples = future.get(5, TimeUnit.SECONDS);
        int numberOfAckedTuples = ackedTuples.size();

        Assert.assertEquals(1, numberOfAckedTuples);
    }

    @Test
    public void worksAfterFrontierRestart()
            throws ExecutionException, InterruptedException, TimeoutException {
        var future1 = store("http://example.com/?test=1", Status.DISCOVERED, new Metadata());
        int numberOfAckedTuples = future1.get(5, TimeUnit.SECONDS).size();
        Assert.assertEquals(1, numberOfAckedTuples);

        urlFrontierContainer.stop();

        final var future2 = store("http://example.com/?test=2", Status.DISCOVERED, new Metadata());
        Assert.assertThrows(TimeoutException.class, () -> future2.get(5, TimeUnit.SECONDS));

        urlFrontierContainer.start();

        // Give the connection checker the time to re-establish the connection to the frontier
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        final var future3 = store("http://example.com/?test=3", Status.DISCOVERED, new Metadata());
        numberOfAckedTuples = future3.get(5, TimeUnit.SECONDS).size();
        Assert.assertEquals(2, numberOfAckedTuples);
    }
}
