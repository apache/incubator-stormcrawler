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

package com.digitalpebble.stormcrawler.cassandra;

import java.util.Date;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.AbstractStatusUpdaterBolt;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.util.URLPartitioner;

public class CassandraStatusUpdater extends AbstractStatusUpdaterBolt {

    private Cluster cluster;
    private PreparedStatement prepared;
    private Session session;
    private URLPartitioner partitioner;
    private PreparedStatement preparedDisco;

    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect();
        partitioner = new URLPartitioner();
        partitioner.configure(stormConf);
        prepared = session
                .prepare("insert into stormcrawler.webpage (url, next_fetch_date, status, hostname, metadata) VALUES (?, ?, ?, ?, ?)");
        preparedDisco = session
                .prepare("insert into stormcrawler.webpage (url, next_fetch_date, status, hostname, metadata) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS");
    }

    @Override
    public void cleanup() {
        if (cluster != null)
            cluster.close();
    }

    @Override
    protected void store(String url, Status status, Metadata md, Date nextFetch)
            throws Exception {
        String partitionKey = partitioner.getPartition(url, md);
        StringBuilder mdAsString = new StringBuilder();
        for (String mdKey : md.keySet()) {
            String[] vals = md.getValues(mdKey);
            for (String v : vals) {
                mdAsString.append("\t").append(mdKey).append("=").append(v);
            }
        }
        if (!status.equals(Status.DISCOVERED)) {
            BoundStatement bound = prepared.bind(url, nextFetch,
                    status.toString(), partitionKey, mdAsString.toString());
            session.execute(bound);
        } else {
            BoundStatement bound = preparedDisco.bind(url, nextFetch,
                    status.toString(), partitionKey, mdAsString.toString());
            session.execute(bound);
        }

    }

}
