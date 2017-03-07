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

package com.digitalpebble.stormcrawler.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.task.WorkerTopologyContext;

import com.digitalpebble.stormcrawler.Constants;
import com.digitalpebble.stormcrawler.Metadata;

@SuppressWarnings("serial")
/**
 * Directs tuples to a specific bolt instance based on the URLPartitioner, e.g.
 * byIP, byDomain or byHost.
 * 
 * Use as follows with Flux :
 * 
 * <pre>
 * {@code
 * streams:
 *  - from: "spout"
 *    to: "status"
 *    grouping:
 *      type: CUSTOM
 *      customClass:
 *        className: "com.digitalpebble.stormcrawler.util.URLStreamGrouping"
 *        constructorArgs:
 *          - "byDomain"
 * }
 * </pre>
 **/
public class URLStreamGrouping implements CustomStreamGrouping, Serializable {

    private int numTasks = 0;
    private URLPartitioner partitioner;

    private String partitionMode;

    public URLStreamGrouping() {
    }

    public URLStreamGrouping(String mode) {
        partitionMode = mode;
    }

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
            List<Integer> targetTasks) {
        numTasks = targetTasks.size();
        partitioner = new URLPartitioner();
        if (StringUtils.isNotBlank(partitionMode)) {
            Map<String, String> conf = new HashMap<>();
            conf.put(Constants.PARTITION_MODEParamName, partitionMode);
            partitioner.configure(conf);
        }
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> boltIds = new LinkedList<>();

        // optimisation : single target
        if (numTasks == 1) {
            boltIds.add(0);
            return boltIds;
        }

        if (values.size() < 2) {
            // TODO log!
            return boltIds;
        }

        // the first value is always the URL
        // and the second the metadata
        String url = (String) values.get(0);
        Metadata metadata = (Metadata) values.get(1);
        String partitionKey = partitioner.getPartition(url, metadata);

        if (StringUtils.isBlank(partitionKey)) {
            // TODO log!
            return boltIds;
        }

        // hash on the key
        int partition = Math.abs(partitionKey.hashCode() % numTasks);
        boltIds.add(partition);

        return boltIds;
    }

}
