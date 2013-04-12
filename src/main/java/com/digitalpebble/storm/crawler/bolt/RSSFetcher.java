package com.digitalpebble.storm.crawler.bolt;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.fetcher.FeedFetcher;
import com.sun.syndication.fetcher.FetcherException;
import com.sun.syndication.fetcher.impl.FeedFetcherCache;
import com.sun.syndication.fetcher.impl.HashMapFeedInfoCache;
import com.sun.syndication.fetcher.impl.HttpURLFeedFetcher;
import com.sun.syndication.io.FeedException;

public class RSSFetcher extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        System.out.println(tuple);

        FeedFetcherCache feedInfoCache = HashMapFeedInfoCache.getInstance();
        FeedFetcher feedFetcher = new HttpURLFeedFetcher(feedInfoCache);
        
        String url = tuple.getStringByField("url");
        
        try {
            SyndFeed feed = feedFetcher
                    .retrieveFeed(new URL(url));
            for (Object entries : feed.getEntries()){
                
            }
        } catch (IllegalArgumentException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (MalformedURLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (FeedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (FetcherException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

}
