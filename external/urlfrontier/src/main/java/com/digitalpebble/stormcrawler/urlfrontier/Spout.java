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

package com.digitalpebble.stormcrawler.urlfrontier;

import java.util.Iterator;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.AbstractQueryingSpout;
import com.digitalpebble.stormcrawler.util.ConfUtils;

import crawlercommons.urlfrontier.URLFrontierGrpc;
import crawlercommons.urlfrontier.URLFrontierGrpc.URLFrontierBlockingStub;
import crawlercommons.urlfrontier.Urlfrontier.GetParams;
import crawlercommons.urlfrontier.Urlfrontier.URLInfo;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

@SuppressWarnings("serial")
public class Spout extends AbstractQueryingSpout {

	public static final Logger LOG = LoggerFactory.getLogger(Spout.class);

	private ManagedChannel channel;

	private URLFrontierBlockingStub blockingFrontier;

	private int maxURLsPerBucket;

	private int maxBucketNum;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

		super.open(conf, context, collector);
		// host and port of URL Frontier

		String host = ConfUtils.getString(conf, "urlfrontier.host", "localhost");
		int port = ConfUtils.getInt(conf, "urlfrontier.port", 7071);

		maxURLsPerBucket = ConfUtils.getInt(conf, "urlfrontier.max.urls.per.bucket", 10);

		maxBucketNum = ConfUtils.getInt(conf, "urlfrontier.max.buckets", 10);

		LOG.info("Initialisation of connection to URLFrontier service on {}:{}", host, port);

		channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
		blockingFrontier = URLFrontierGrpc.newBlockingStub(channel);
	}

	@Override
	protected void populateBuffer() {
		GetParams request = GetParams.newBuilder().setMaxUrlsPerQueue(maxURLsPerBucket).setMaxQueues(maxBucketNum)
				.build();
		try {
			Iterator<URLInfo> iter = blockingFrontier.getURLs(request);
			while (iter.hasNext()) {
				URLInfo item = iter.next();
				Metadata m = new Metadata();
				item.getMetadataMap().forEach((k, v) -> {
					for (int index = 0; index < v.getValuesCount(); index++) {
						m.addValue(k, v.getValues(index));
					}
				});
				buffer.add(item.getUrl(), m, item.getKey());
			}
		} catch (Throwable e) {
			// server inaccessible?
			LOG.error("Exception caught {}", e.getMessage());
		}
	}

	@Override
	public void ack(Object msgId) {
		LOG.debug("Ack for {}", msgId);
		super.ack(msgId);
	}

	@Override
	public void fail(Object msgId) {
		LOG.info("Fail for {}", msgId);
		super.fail(msgId);
	}

	@Override
	public void close() {
		super.close();
		LOG.info("Shutting down connection to URLFrontier service");
		channel.shutdown();
	}
}
