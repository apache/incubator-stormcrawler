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

package com.digitalpebble.storm.crawler.librato.metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.librato.metrics.HttpPoster;
import com.librato.metrics.HttpPoster.Response;
import com.librato.metrics.LibratoBatch;
import com.librato.metrics.NingHttpPoster;
import com.librato.metrics.Sanitizer;
import com.librato.metrics.Versions;

/**
 * Sends the metrics to Librato
 */
public class LibratoMetricsConsumer implements IMetricsConsumer {

    private static final int DEFAULT_BATCH_SIZE = 500;
    private static final Logger LOG = LoggerFactory
            .getLogger(LibratoMetricsConsumer.class);
    private static final String LIB_VERSION = Versions.getVersion(
            "META-INF/maven/com.librato.metrics/librato-java/pom.properties",
            LibratoBatch.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final Sanitizer sanitizer = new Sanitizer() {
        @Override
        public String apply(String name) {
            return Sanitizer.LAST_PASS.apply(name);
        }
    };

    private int postBatchSize = DEFAULT_BATCH_SIZE;
    private long timeout = 30;
    private final TimeUnit timeoutUnit = TimeUnit.SECONDS;
    private String userAgent = null;
    private HttpPoster httpPoster;

    private Set<String> metricsToKeep = new HashSet<String>();

    @Override
    public void prepare(Map stormConf, Object registrationArgument,
            TopologyContext context, IErrorReporter errorReporter) {

        // TODO configure timeouts
        // this.timeout = timeout;
        // this.timeoutUnit = timeoutUnit;
        // this.postBatchSize = postBatchSize;

        String agentIdentifier = (String) stormConf.get("librato.agent");
        if (agentIdentifier == null)
            agentIdentifier = "storm";

        String token = (String) stormConf.get("librato.token");

        String username = (String) stormConf.get("librato.username");

        String apiUrl = (String) stormConf.get("librato.api.url");

        if (apiUrl == null)
            apiUrl = "https://metrics-api.librato.com/v1/metrics";

        // check that the values are not null
        if (StringUtils.isBlank(token))
            throw new RuntimeException("librato.token not set");

        if (StringUtils.isBlank(username))
            throw new RuntimeException("librato.username not set");

        this.userAgent = String.format("%s librato-java/%s", agentIdentifier,
                LIB_VERSION);

        this.httpPoster = NingHttpPoster.newPoster(username, token, apiUrl);

        // get the list of metrics names to keep if any
        String metrics2keep = (String) stormConf.get("librato.metrics.to.keep");

        if (metrics2keep != null) {
            String[] mets = metrics2keep.split(",");
            for (String m : mets)
                metricsToKeep.add(m.trim().toLowerCase());
        }

    }

    // post(String source, long epoch)
    @Override
    public void handleDataPoints(TaskInfo taskInfo,
            Collection<DataPoint> dataPoints) {

        final Map<String, Object> payloadMap = new HashMap<String, Object>();

        payloadMap.put("source", taskInfo.srcComponentId + "_"
                + taskInfo.srcWorkerHost + "_" + taskInfo.srcTaskId);
        payloadMap.put("measure_time", taskInfo.timestamp);

        final List<Map<String, Object>> gaugeData = new ArrayList<Map<String, Object>>();
        final List<Map<String, Object>> counterData = new ArrayList<Map<String, Object>>();

        int counter = 0;

        final Iterator<DataPoint> datapointsIterator = dataPoints.iterator();
        while (datapointsIterator.hasNext()) {
            final DataPoint dataPoint = datapointsIterator.next();

            // ignore datapoint with a value which is not a map
            if (!(dataPoint.value instanceof Map))
                continue;

            // a counter or a gauge
            // convention if its name contains '_counter'
            // then treat it as a counter
            boolean isCounter = false;

            if (dataPoint.name.contains("_counter")) {
                isCounter = true;
                dataPoint.name = dataPoint.name.replaceFirst("_counter", "");
            }

            if (!metricsToKeep.isEmpty()) {
                if (!metricsToKeep.contains(dataPoint.name.toLowerCase())) {
                    continue;
                }
            }

            try {
                Map<String, Number> metric = (Map<String, Number>) dataPoint.value;
                for (Map.Entry<String, Number> entry : metric.entrySet()) {
                    String metricId = entry.getKey();
                    Number val = entry.getValue();

                    final Map<String, Object> data = new HashMap<String, Object>();
                    data.put("name",
                            sanitizer.apply(dataPoint.name + "_" + metricId));
                    data.put("value", val);

                    if (isCounter)
                        counterData.add(data);
                    else
                        // use as gauge
                        gaugeData.add(data);

                    counter++;
                    if (counter % postBatchSize == 0
                            || (!datapointsIterator.hasNext() && (!counterData
                                    .isEmpty() || !gaugeData.isEmpty()))) {
                        final String countersKey = "counters";
                        final String gaugesKey = "gauges";

                        payloadMap.put(countersKey, counterData);
                        payloadMap.put(gaugesKey, gaugeData);
                        postPortion(payloadMap);
                        payloadMap.remove(gaugesKey);
                        payloadMap.remove(countersKey);
                        gaugeData.clear();
                        counterData.clear();
                    }
                }
            } catch (RuntimeException e) {
                LOG.error(e.getMessage());
            }

        }
        LOG.debug("Posted {} measurements", counter);

    }

    @Override
    public void cleanup() {

    }

    private void postPortion(Map<String, Object> chunk) {
        try {
            final String payload = OBJECT_MAPPER.writeValueAsString(chunk);
            final Future<Response> future = httpPoster.post(userAgent, payload);
            final Response response = future.get(timeout, timeoutUnit);
            final int statusCode = response.getStatusCode();
            if (statusCode < 200 || statusCode >= 300) {
                LOG.error(
                        "Received an error from Librato API. Code : {}, Message: {}",
                        statusCode, response.getBody());
            }
        } catch (Exception e) {
            LOG.error("Unable to post to Librato API", e);
        }
    }
}
