/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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

package com.digitalpebble.storm.crawler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import com.digitalpebble.storm.crawler.util.Configuration;
import com.digitalpebble.storm.fetchqueue.ShardedQueue;

/**
 * A simple client which puts URLS in a sharded queue so that they get processed
 * with the storm pipeline
 **/

public class URLInjector {

	private ShardedQueue queue;

	URLInjector() throws Exception {
		Configuration config = StormConfiguration.create();
		queue = ShardedQueue.getInstance(config);
	}

	public void add(String url) {
		try {
			queue.add(url);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void close() {
		queue.close();
	}

	public static void main(String[] args) throws Exception {
		String messages = args[0];
		URLInjector client = new URLInjector();
		BufferedReader reader = new BufferedReader(new FileReader(new File(
				messages)));
		String line = null;
		while ((line = reader.readLine()) != null) {
			client.add(line.trim());
		}
		reader.close();
		client.close();
	}

}
