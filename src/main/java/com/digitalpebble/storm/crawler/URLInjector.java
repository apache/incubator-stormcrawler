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
