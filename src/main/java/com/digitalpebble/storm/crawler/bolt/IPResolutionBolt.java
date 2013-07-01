package com.digitalpebble.storm.crawler.bolt;

import java.net.InetAddress;
import java.net.URL;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class IPResolutionBolt extends BaseRichBolt {

    public static final Logger LOG = LoggerFactory
            .getLogger(IPResolutionBolt.class);

    OutputCollector _collector;

    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url");
        String ip = null;
        String host = "null";
        try {
            URL u = new URL(url);
            host = u.getHost();
            long start = System.currentTimeMillis();
            final InetAddress addr = InetAddress.getByName(u.getHost());
            ip = addr.getHostAddress();
            long end = System.currentTimeMillis();

            LOG.info("IP for: " + host + " > " + ip + " in "+(end-start)+ " msec");

            _collector.emit(new Values(url, ip));
            _collector.ack(tuple);
        } catch (final Exception e) {
            LOG.warn("Unable to resolve IP for: " + host);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "ip"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        _collector = collector;
    }

}
