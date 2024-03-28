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
package org.apache.stormcrawler.opensearch.bolt;

import org.junit.After;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public abstract class AbstractOpenSearchTest {

    private static final String OPENSEARCH_VERSION = "2.12.0";

    public static final String PASSWORD = "This1sAPassw0rd";

    @Rule public Timeout globalTimeout = Timeout.seconds(120);

    @Rule
    public GenericContainer opensearchContainer =
            new GenericContainer(
                            DockerImageName.parse(
                                    "opensearchproject/opensearch:" + OPENSEARCH_VERSION))
                    .withExposedPorts(9200)
                    .withEnv("plugins.security.disabled", "true")
                    .withEnv("discovery.type", "single-node")
                    .withEnv("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
                    .withEnv("OPENSEARCH_INITIAL_ADMIN_PASSWORD", PASSWORD);

    @After
    public void close() {
        opensearchContainer.close();
    }
}
