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

package com.digitalpebble.stormcrawler.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.metric.api.MultiReducedMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.AbstractStatusUpdaterBolt;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.URLPartitioner;

@SuppressWarnings("serial")
public class StatusUpdaterBolt extends AbstractStatusUpdaterBolt {

    public static final Logger LOG = LoggerFactory
            .getLogger(StatusUpdaterBolt.class);

    private MultiReducedMetric averagedMetrics;
    private MultiCountMetric eventCounter;

    private Connection connection;
    private String tableName;

    private URLPartitioner partitioner;
    private int maxNumBuckets = -1;

    public StatusUpdaterBolt(int maxNumBuckets) {
        this.maxNumBuckets = maxNumBuckets;
    }

    /** Does not shard based on the total number of queues **/
    public StatusUpdaterBolt() {
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        super.prepare(stormConf, context, collector);

        partitioner = new URLPartitioner();
        partitioner.configure(stormConf);

        this.averagedMetrics = context.registerMetric("SQLStatusUpdater",
                new MultiReducedMetric(new MeanReducer()), 10);

        this.eventCounter = context.registerMetric("counter",
                new MultiCountMetric(), 10);

        tableName = ConfUtils.getString(stormConf,
                Constants.MYSQL_TABLE_PARAM_NAME);

        try {
            connection = SQLUtil.getConnection(stormConf);
        } catch (SQLException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }

    }

    @Override
    public void store(String url, Status status, Metadata metadata,
            Date nextFetch) throws Exception {

        // the mysql insert statement
        String query = tableName
                + " (url, status, nextfetchdate, metadata, bucket, host)"
                + " values (?, ?, ?, ?, ?, ?)";

        StringBuffer mdAsString = new StringBuffer();
        for (String mdKey : metadata.keySet()) {
            String[] vals = metadata.getValues(mdKey);
            for (String v : vals) {
                mdAsString.append("\t").append(mdKey).append("=").append(v);
            }
        }

        int partition = 0;
        String partitionKey = partitioner.getPartition(url, metadata);
        if (maxNumBuckets > 1) {
            // determine which shard to send to based on the host / domain / IP
            partition = Math.abs(partitionKey.hashCode() % maxNumBuckets);
        }

        // create in table if does not already exist
        if (status.equals(Status.DISCOVERED)) {
            query = "INSERT IGNORE INTO " + query;
        } else
            query = "REPLACE INTO " + query;

        PreparedStatement preparedStmt = connection.prepareStatement(query);
        preparedStmt.setString(1, url);
        preparedStmt.setString(2, status.toString());
        preparedStmt.setObject(3, nextFetch);
        preparedStmt.setString(4, mdAsString.toString());
        preparedStmt.setInt(5, partition);
        preparedStmt.setString(6, partitionKey);

        long start = System.currentTimeMillis();

        // execute the preparedstatement
        preparedStmt.execute();
        eventCounter.scope("sql_query_number").incrBy(1);
        averagedMetrics.scope("sql_execute_time").update(
                System.currentTimeMillis() - start);
        preparedStmt.close();
    }
}