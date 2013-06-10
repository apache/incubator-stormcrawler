package com.digitalpebble.storm.crawler.bolt.indexing;

import java.util.Iterator;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/** Dummy indexer which displays the fields on the std out **/

@SuppressWarnings("serial")
public class PrinterBolt extends BaseRichBolt {
	OutputCollector _collector;

	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
	}

	public void execute(Tuple tuple) {
		Iterator<String> iterator = tuple.getFields().iterator();
		while (iterator.hasNext()) {
			String fieldName = iterator.next();
			Object obj = tuple.getValueByField(fieldName);

			if (obj instanceof byte[])
				System.out.println(fieldName + "\t"
						+ tuple.getBinaryByField(fieldName).length + " bytes");
			else {
				String value = tuple.getValueByField(fieldName).toString();
				if (value.length() > 100) {
					System.out.println(fieldName + "\t" + value.length()
							+ " chars");
				} else
					System.out.println(fieldName + "\t"
							+ tuple.getValueByField(fieldName));

			}

		}
		_collector.ack(tuple);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}