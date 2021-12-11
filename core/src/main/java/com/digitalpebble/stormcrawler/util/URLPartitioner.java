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
package com.digitalpebble.stormcrawler.util;

import com.digitalpebble.stormcrawler.Metadata;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates a partition key for a given URL based on the hostname, domain or IP address. This can
 * be called by the URLPartitionerBolt or any other component.
 */
public class URLPartitioner {

    private static final Logger LOG = LoggerFactory.getLogger(URLPartitioner.class);

    private PartitionMode mode = PartitionMode.QUEUE_MODE_DOMAIN;

    /**
     * Returns the host, domain, IP of a URL so that it can be partitioned for politeness, depending
     * on the value of the config <i>partition.url.mode</i>.
     */
    @Nullable
    public String getPartition(@NotNull String url, @NotNull Metadata metadata) {

        String partitionKey = null;

        // IP in metadata?
        if (mode == PartitionMode.QUEUE_MODE_IP) {
            String ip_provided = metadata.getFirstValue("ip");
            if (StringUtils.isNotBlank(ip_provided)) {
                partitionKey = ip_provided;
            }
        }

        if (partitionKey == null) {
            URL u;
            try {
                u = new URL(url);
            } catch (MalformedURLException e1) {
                LOG.warn("Invalid URL: {}", url);
                return null;
            }
            partitionKey = PartitionUtil.getPartitionKeyByMode(u, mode);
        }

        LOG.debug("Partition Key for: {} > {}", url, partitionKey);

        return partitionKey;
    }

    public void configure(Map<String, Object> stormConf) {
        mode = PartitionUtil.readPartitionModeForPartitioner(stormConf);
        LOG.info("Using partition mode : {}", mode);
    }
}
