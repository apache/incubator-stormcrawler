package com.digitalpebble.storm.crawler.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import crawlercommons.fetcher.FetchedResult;
import crawlercommons.fetcher.http.SimpleHttpFetcher;
import crawlercommons.fetcher.http.UserAgent;

/*** Prototype of a fetcher that keeps internal queues of what to fetch next ***/
@SuppressWarnings("serial")
public class FetchUrlBolt extends BaseRichBolt implements Runnable {
    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(FetchUrlBolt.class);

    private static final long MIN_SLEEP_TIME = 0;

    private static final long MAX_SLEEP_TIME = 100;

    // TODO use native long/long for efficiency
    // TODO all structures must be thread-safe
    private transient Map<Long, Long> _ipToFetchTime;
    private transient Queue<Tuple> _fetchNow;
    private transient Queue<Tuple> _fetchLater;

    private transient Thread _backgroundFetcher;
    private transient SimpleHttpFetcher _fetcher;

    private transient OutputCollector _collector;

    public FetchUrlBolt() {
        super();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        _collector = collector;

        // TODO use type that supports separate sorted list, keyed
        // by fetch time
        _ipToFetchTime = new HashMap<Long, Long>();

        // TODO set appropriate capacity from constructor parameter
        _fetchNow = new ArrayBlockingQueue<Tuple>(1000);

        // TODO set appropriate capacity from constructor parameter
        _fetchLater = new ArrayBlockingQueue<Tuple>(1000);

        // TODO pass in fetcher policy, user agent info to constructor.
        _fetcher = new SimpleHttpFetcher(1, new UserAgent("test",
                "email@domain.com", "http://domain.com"));

        // Set up thread that is constantly fetching items.
        _backgroundFetcher = new Thread(this);
        _backgroundFetcher.start();

        // TODO Auto-generated method stub
        // what else do I need to do here? What was BaseBasicBolt do?

    }

    @Override
    public void execute(Tuple input) {
        String url = input.getStringByField("url");
        // long crawlDelay = input.getLongByField("crawldelay");
        String ip = input.getStringByField("ip");

        // TODO handle offer failing, because either queue is full
        if (!_ipToFetchTime.containsKey(ip)) {
            _fetchNow.offer(input);
        } else {
            long targetTime = _ipToFetchTime.get(ip);
            if (targetTime <= System.currentTimeMillis()) {
                _fetchNow.offer(input);
            } else {
                _fetchLater.offer(input);
            }
        }

        // There's a separate background process running that checks for entries
        // in the _fetchNow queue, and fetches them right away. Otherwise it
        // sees if any of the times have expired in the _ipToFetchTime, and if
        // so it removes that entry, and moves the first matching entry in the
        // fetchLater
        // queue into the fetchNow queue.
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO should we emit all fields from FetchResult? Probably not, but
        // likely we'd want a few more, like "fetchedUrl", "newUrl".
        declarer.declare(new Fields("url", "content"));
    }

    @Override
    public void cleanup() {
        super.cleanup();

        _backgroundFetcher.interrupt();

        // TODO - drain the queues (set all to skipped, due to termination)
    }

    @Override
    public void run() {

        while (!Thread.interrupted()) {
            long sleepTime = MAX_SLEEP_TIME;
            Tuple t = _fetchNow.poll();

            if (t == null) {
                // TODO see if we have an unblocked IP addresses (time has
                // expired).
                // If so, set sleep time to min, then remove it, and see if we
                // have a URL with the same IP address
                // If so, that's our FetchDatum.
                // FUTURE get up to N of these, for a batch fetch using
                // keep-alive.
            }

            if (t != null) {
                sleepTime = MIN_SLEEP_TIME;
                String url = t.getStringByField("url");
                try {
                    FetchedResult result = _fetcher.fetch(url);
                    // emit results
                    // _collector.emit(anchor, tuple);
                    final byte[] content = result.getContent();
                    final int statusCode = result.getStatusCode();

                    LOG.info("Fetched " + url + " with status " + statusCode);

                    _collector.emit(new Values(url, content));
                    _collector.ack(t);
                } catch (Exception e) {
                }
            }

            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

    }

}
