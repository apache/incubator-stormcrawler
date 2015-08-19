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

package com.digitalpebble.storm.crawler.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.persistence.AbstractStatusUpdaterBolt;
import com.digitalpebble.storm.crawler.persistence.Status;
import com.digitalpebble.storm.crawler.util.ConfUtils;
import com.digitalpebble.storm.crawler.util.URLPartitioner;

@SuppressWarnings("serial")
public class StatusUpdaterBolt extends AbstractStatusUpdaterBolt {

    public static final Logger LOG = LoggerFactory
            .getLogger(StatusUpdaterBolt.class);

    private Connection con;
    private String tableName;

    private URLPartitioner partitioner;
    private int maxNumQueues = 1;

    public StatusUpdaterBolt(int maxNumQueues) {
        this.maxNumQueues = maxNumQueues;
    }

    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        super.prepare(stormConf, context, collector);

        partitioner = new URLPartitioner();
        partitioner.configure(stormConf);

        // SQL connection details
        String url = ConfUtils.getString(stormConf,
                Constants.MYSQL_URL_PARAM_NAME,
                "jdbc:mysql://localhost:3306/crawl");
        String user = ConfUtils.getString(stormConf,
                Constants.MYSQL_USER_PARAM_NAME);
        String password = ConfUtils.getString(stormConf,
                Constants.MYSQL_PASSWORD_PARAM_NAME);
        tableName = ConfUtils.getString(stormConf,
                Constants.MYSQL_TABLE_PARAM_NAME);
        try {
            con = DriverManager.getConnection(url, user, password);
        } catch (SQLException ex) {
            LOG.error(ex.getMessage(), ex);
        }
        // TODO check that the table already exists
    }

    @Override
    public void store(String url, Status status, Metadata metadata,
            Date nextFetch) throws Exception {
        // TODO WHAT TO DO BASED ON STATUS
        // INSERT IGNORE INTO

        // TODO sharding? do that in field?

        // the mysql insert statement
        String query = tableName + " (url, nextfetchdate, metadata, bucket)"
                + " values (?, ?, ?, ?)";

        StringBuffer mdAsString = new StringBuffer();
        for (String mdKey : metadata.keySet()) {
            String[] vals = metadata.getValues(mdKey);
            for (String v : vals) {
                mdAsString.append("\t").append(mdKey).append("=").append(v);
            }
        }

        // determine which queue to send to based on the host / domain / IP
        String partitionKey = partitioner.getPartition(url, metadata);
        int partition = Math.abs(partitionKey.hashCode() % maxNumQueues);

        // create in table if does not already exist
        if (status.equals(Status.DISCOVERED)) {
            query = "INSERT IGNORE INTO " + query;
        } else
            query = "REPLACE INTO " + query;

        PreparedStatement preparedStmt = con.prepareStatement(query);
        preparedStmt.setString(1, url);
        preparedStmt.setObject(2, nextFetch);
        preparedStmt.setString(3, mdAsString.toString());
        preparedStmt.setInt(4, partition);

        // execute the preparedstatement
        preparedStmt.execute();

    }
}