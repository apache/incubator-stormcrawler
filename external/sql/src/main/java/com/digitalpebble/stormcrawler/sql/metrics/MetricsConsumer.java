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
package com.digitalpebble.stormcrawler.sql.metrics;

import com.digitalpebble.stormcrawler.sql.Constants;
import com.digitalpebble.stormcrawler.sql.SQLUtil;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsConsumer implements IMetricsConsumer {

    private final Logger LOG = LoggerFactory.getLogger(getClass());
    private Connection connection;
    private String query;

    @Override
    public void prepare(
            Map stormConf,
            Object registrationArgument,
            TopologyContext context,
            IErrorReporter errorReporter) {
        final String tableName =
                ConfUtils.getString(stormConf, Constants.SQL_METRICS_TABLE_PARAM_NAME, "metrics");
        query =
                "INSERT INTO "
                        + tableName
                        + " (srcComponentId, srcTaskId, srcWorkerHost, srcWorkerPort, name, value, timestamp)"
                        + " values (?, ?, ?, ?, ?, ?, ?)";
        try {
            connection = SQLUtil.getConnection(stormConf);
        } catch (SQLException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        final Date now = new Date();
        try {
            PreparedStatement preparedStmt = connection.prepareStatement(query);
            for (DataPoint dataPoint : dataPoints) {
                handleDataPoints(preparedStmt, taskInfo, dataPoint.name, dataPoint.value, now);
            }
            preparedStmt.executeBatch();
            preparedStmt.close();
        } catch (SQLException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }

    private void handleDataPoints(
            final PreparedStatement preparedStmt,
            final TaskInfo taskInfo,
            final String nameprefix,
            final Object value,
            final Date now) {
        if (value instanceof Number) {
            try {
                indexDataPoint(
                        preparedStmt, taskInfo, now, nameprefix, ((Number) value).doubleValue());
            } catch (SQLException e) {
                LOG.error("Exception while indexing datapoint", e);
            }
        } else if (value instanceof Map) {
            Iterator<Entry> keyValiter = ((Map) value).entrySet().iterator();
            while (keyValiter.hasNext()) {
                Entry entry = keyValiter.next();
                String newnameprefix = nameprefix + "." + entry.getKey();
                handleDataPoints(preparedStmt, taskInfo, newnameprefix, entry.getValue(), now);
            }
        } else if (value instanceof Collection) {
            for (Object collectionObj : (Collection) value) {
                handleDataPoints(preparedStmt, taskInfo, nameprefix, collectionObj, now);
            }
        } else {
            LOG.warn("Found data point value {} of {}", nameprefix, value.getClass().toString());
        }
    }

    private void indexDataPoint(
            final PreparedStatement preparedStmt,
            TaskInfo taskInfo,
            Date timestamp,
            String name,
            double value)
            throws SQLException {
        preparedStmt.setString(1, taskInfo.srcComponentId);
        preparedStmt.setInt(2, taskInfo.srcTaskId);
        preparedStmt.setString(3, taskInfo.srcWorkerHost);
        preparedStmt.setInt(4, taskInfo.srcWorkerPort);
        preparedStmt.setString(5, name);
        preparedStmt.setDouble(6, value);
        preparedStmt.setObject(7, timestamp);
        preparedStmt.addBatch();
    }

    @Override
    public void cleanup() {
        try {
            connection.close();
        } catch (SQLException e) {
        }
    }
}
