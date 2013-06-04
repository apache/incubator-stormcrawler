package com.digitalpebble.storm.crawler.bolt;

import java.util.Iterator;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

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
            if (obj instanceof String)
            System.out.println(fieldName+"\t"+tuple.getValueByField(fieldName));
            else 
                System.out.println(fieldName+"\t"+tuple.getBinaryByField(fieldName).length + "bytes");

        }
        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}