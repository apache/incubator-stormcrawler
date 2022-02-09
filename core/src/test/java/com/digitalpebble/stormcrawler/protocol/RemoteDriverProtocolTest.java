/*
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
package com.digitalpebble.stormcrawler.protocol;

import static org.junit.Assert.fail;

import com.digitalpebble.stormcrawler.protocol.selenium.RemoteDriverProtocol;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.URLResolver;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import org.apache.storm.Config;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class RemoteDriverProtocolTest {

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<Object> parametersForTesting() {
        try {

            return Arrays.asList(
                    new Object[][] {
                        {
                            "selenium/selenium_mixed_config.yaml",
                            new URL[] {
                                new URL("http://stormcrawler.net/"),
                                new URL("http://stormcrawler.net/"),
                                new URL("http://stormcrawler.net/"),
                                new URL("http://127.0.0.1:4444/"),
                                new URL("http://127.0.0.1:4444/"),
                                new URL("http://127.0.0.1:4444/"),
                                new URL("http://view-localhost:4444"),
                                new URL("http://localhost:4444/"),
                            }
                        },
                        {
                            "selenium/selenium_single_simple_config.yaml",
                            new URL[] {
                                new URL("http://stormcrawler.net/"),
                            }
                        },
                        {
                            "selenium/selenium_multiple_simple_config.yaml",
                            new URL[] {
                                new URL("http://stormcrawler.net/"),
                                new URL("https://google.de/"),
                                new URL("https://amazon.de/"),
                            }
                        },
                        {
                            "selenium/selenium_single_complex_config.yaml",
                            new URL[] {
                                new URL("http://127.0.0.1:9000/"),
                            }
                        },
                        {
                            "selenium/selenium_multiple_complex_config.yaml",
                            new URL[] {
                                new URL("http://127.0.0.1:9000/"),
                                new URL("http://stormcrawler.net/"),
                                new URL("http://127.0.0.1:5555/"),
                            }
                        },
                    });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Parameterized.Parameter public String pathToConfig;

    @Parameterized.Parameter(1)
    public Object[] expected;

    @Test
    public void test_configs() {
        Config config = new Config();
        ConfUtils.loadConfigIntoTarget(pathToConfig, config);
        List<URLResolver.ResolvedUrl> urls;
        try {
            urls = RemoteDriverProtocol.loadURLsFromConfig(config);
        } catch (MalformedURLException e) {
            fail("There shouldn't be any malformed urls.");
            throw new RuntimeException(e);
        }

        Assert.assertNotNull(urls);

        Assert.assertArrayEquals(
                expected, urls.stream().map(URLResolver.ResolvedUrl::getResolved).toArray());
    }
}
