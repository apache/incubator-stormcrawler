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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

import com.digitalpebble.storm.crawler.util.ConfUtils;
import com.digitalpebble.storm.crawler.util.StringTabScheme;

public class SQLSpout extends BaseRichSpout {

    public static final Logger LOG = LoggerFactory.getLogger(SQLSpout.class);

    private SpoutOutputCollector _collector;

    private Scheme _scheme = new StringTabScheme();

    private String tableName;

    private Connection connection;

    // TODO set via config
    private final int bufferSize = 100;

    private Queue<List<Object>> buffer = new LinkedList<List<Object>>();

    @Override
    public void open(Map conf, TopologyContext context,
            SpoutOutputCollector collector) {
        _collector = collector;

        // SQL connection details
        String url = ConfUtils.getString(conf, Constants.MYSQL_URL_PARAM_NAME,
                "jdbc:mysql://localhost:3306/crawl");
        String user = ConfUtils
                .getString(conf, Constants.MYSQL_USER_PARAM_NAME);
        String password = ConfUtils.getString(conf,
                Constants.MYSQL_PASSWORD_PARAM_NAME);
        tableName = ConfUtils.getString(conf, Constants.MYSQL_TABLE_PARAM_NAME);
        try {
            connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException ex) {
            LOG.error(ex.getMessage(), ex);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_scheme.getOutputFields());
    }

    @Override
    public void nextTuple() {
        if (!buffer.isEmpty()) {
            List<Object> fields = buffer.remove();
            String url = fields.get(0).toString();
            this._collector.emit(fields, url);
            return;
        }

        // re-populate the buffer
        populateBuffer();
    }

    private void populateBuffer() {
        // select entries from mysql
        String query = "SELECT * FROM " + tableName;
        query += " WHERE nextfetchdate <= '"
                + new Timestamp(new Date().getTime()) + "'";
        query += " LIMIT " + this.bufferSize;

        // TODO ensure diversity based on bucket field

        // create the java statement
        Statement st = null;
        try {
            st = this.connection.createStatement();

            // execute the query, and get a java resultset
            ResultSet rs = st.executeQuery(query);

            // iterate through the java resultset
            while (rs.next()) {
                String url = rs.getString("url");
                String metadata = rs.getString("metadata");
                String URLMD = url + metadata;
                List<Object> v = _scheme.deserialize(URLMD.getBytes());
                buffer.add(v);
            }
        } catch (SQLException e) {
            LOG.error("Exception while querying table", e);
        } finally {
            try {
                if (st != null)
                    st.close();
            } catch (SQLException e) {
                LOG.error("Exception closing statement", e);
            }
        }
    }
}
