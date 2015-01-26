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

package com.digitalpebble.storm.crawler.metrics;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.storm.guava.base.Joiner;
import org.apache.storm.guava.collect.ImmutableMap;
import org.apache.storm.guava.collect.ImmutableSortedMap;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

/**
 * Displays metrics at JSON format in a servlet running on
 * http://localhost:7070/metrics
 *
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class DebugMetricsConsumer implements IMetricsConsumer {
    private static final Logger log = LoggerFactory
            .getLogger(DebugMetricsConsumer.class);
    private IErrorReporter errorReporter;
    private Server server;

    // Make visible to servlet threads
    private volatile ConcurrentMap<String, Number> metrics;
    private volatile ConcurrentMap<String, Map<String, Object>> metrics_metadata;

    @Override
    public void prepare(Map stormConf, Object registrationArgument,
            TopologyContext context, IErrorReporter errorReporter) {
        this.errorReporter = errorReporter;
        this.metrics = new ConcurrentHashMap<String, Number>();
        this.metrics_metadata = new ConcurrentHashMap<String, Map<String, Object>>();

        try {
            // TODO Config file not tested
            final String PORT_CONFIG_STRING = "topology.metrics.consumers.debug.servlet.port";
            Integer port = (Integer) stormConf.get(PORT_CONFIG_STRING);
            if (port == null) {
                log.warn("Metrics debug servlet's port not specified, defaulting to 7070. You can specify it via "
                        + PORT_CONFIG_STRING + " in storm.yaml");
                port = 7070;
            }
            server = startServlet(port);
        } catch (Exception e) {
            log.error("Failed to start metrics server", e);
            throw new AssertionError(e);
        }
    }

    private static final Joiner ON_COLONS = Joiner.on("::");

    @Override
    public void handleDataPoints(TaskInfo taskInfo,
            Collection<DataPoint> dataPoints) {
        // In order
        String componentId = taskInfo.srcComponentId;
        Integer taskId = taskInfo.srcTaskId;
        Integer updateInterval = taskInfo.updateIntervalSecs;
        Long timestamp = taskInfo.timestamp;
        for (DataPoint point : dataPoints) {
            String metric_name = point.name;
            try {
                Map<String, Number> metric = (Map<String, Number>) point.value;
                for (Map.Entry<String, Number> entry : metric.entrySet()) {
                    String metricId = ON_COLONS.join(componentId, taskId,
                            metric_name, entry.getKey());
                    Number val = entry.getValue();
                    metrics.put(metricId, val);
                    metrics_metadata.put(metricId, ImmutableMap
                            .<String, Object> of("updateInterval",
                                    updateInterval, "lastreported", timestamp));
                }
            } catch (RuntimeException e) {
                // One can easily send something else than a Map<String,Number>
                // down the __metrics stream and make this part break.
                // If you ask me either the message should carry type
                // information or there should be different stream per message
                // type
                // This is one of the reasons why I want to write a further
                // abstraction on this facility
                errorReporter.reportError(e);
                metrics_metadata
                        .putIfAbsent("ERROR_METRIC_CONSUMER_"
                                + e.getClass().getSimpleName(), ImmutableMap
                                .of("offending_message_sample", point.value));
            }
        }
    }

    private static final ObjectMapper OM = new ObjectMapper();

    private Server startServlet(int serverPort) throws Exception {
        // Setup HTTP server
        Server server = new Server(serverPort);
        Context root = new Context(server, "/");
        server.start();

        HttpServlet servlet = new HttpServlet() {
            @Override
            protected void doGet(HttpServletRequest req,
                    HttpServletResponse resp) throws ServletException,
                    IOException {
                SortedMap<String, Number> metrics = ImmutableSortedMap
                        .copyOf(DebugMetricsConsumer.this.metrics);
                SortedMap<String, Map<String, Object>> metrics_metadata = ImmutableSortedMap
                        .copyOf(DebugMetricsConsumer.this.metrics_metadata);

                Map<String, Object> toplevel = ImmutableMap
                        .of("retrieved",
                                new Date(),

                                // TODO this call fails with mysterious
                                // exception
                                // "java.lang.IllegalArgumentException: Could not find component common for __metrics"
                                // Mailing list suggests it's a library version
                                // issue but couldn't find anything suspicious
                                // Need to eventually investigate
                                // "sources",
                                // context.getThisSources().toString(),

                                "metrics", metrics, "metric_metadata",
                                metrics_metadata);

                ObjectWriter prettyPrinter = OM
                        .writerWithDefaultPrettyPrinter();
                prettyPrinter.writeValue(resp.getWriter(), toplevel);
            }
        };

        root.addServlet(new ServletHolder(servlet), "/metrics");

        log.info("Started metric server...");
        return server;

    }

    @Override
    public void cleanup() {
        try {
            server.stop();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

}
