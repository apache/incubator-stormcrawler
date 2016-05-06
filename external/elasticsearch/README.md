storm-crawler-elasticsearch
===========================

A collection of resources for [Elasticsearch](https://www.elastic.co/products/elasticsearch):
* [IndexerBolt](https://github.com/DigitalPebble/storm-crawler/blob/master/external/elasticsearch/src/main/java/com/digitalpebble/storm/crawler/elasticsearch/bolt/IndexerBolt.java) for indexing documents fetched with StormCrawler
* [Spout](https://github.com/DigitalPebble/storm-crawler/blob/master/external/elasticsearch/src/main/java/com/digitalpebble/storm/crawler/elasticsearch/persistence/ElasticSearchSpout.java) and [StatusUpdaterBolt](https://github.com/DigitalPebble/storm-crawler/blob/master/external/elasticsearch/src/main/java/com/digitalpebble/storm/crawler/elasticsearch/persistence/StatusUpdaterBolt.java) for persisting URL information in recursive crawls
* [MetricsConsumer](https://github.com/DigitalPebble/storm-crawler/blob/master/external/elasticsearch/src/main/java/com/digitalpebble/storm/crawler/elasticsearch/metrics/MetricsConsumer.java) 

as well as examples of crawl and injection topologies.

We also have resources for [Kibana](https://www.elastic.co/products/kibana) to build basic real-time monitoring dashboards for the crawls, such as the one below.

![bla](https://pbs.twimg.com/media/CR1-waVWEAAh0u4.png)  

Getting started
---------------------

We'll assume that Elasticsearch and Kibana are installed and running on your machine. You'll also need Java, Maven and Storm installed.

First compile the code for the ElasticSearch module with `mvn clean install -P bigjar`.

Then we run the script `ES_IndexInit.sh`, which creates 2 indices : one for persisting the status of URLs (_status_) and one for persisting the Storm metrics (_metrics_). A third index (_index_) for searching the documents fetched by stormcrawler will be created automatically by the topology, you should probably tune its mapping later on.

We can either inject seed URLs directly into the _status_ index \:

```
now=`date -Iseconds`

url='http%3A%2F%2Fwww.theguardian.com%2Fnewssitemap.xml'

curl -XPUT "http://localhost:9200/status/status/$url/_create" -d '{
  "url": "http://www.theguardian.com/newssitemap.xml",
  "status": "DISCOVERED",
  "nextFetchDate": "'$now'"
  }
}'
```

or put the seed in a text file with one URL per line e.g.

`echo 'http://www.theguardian.com/newssitemap.xml' > seeds.txt`

then call the ESSeedInjector topology with 

`storm jar target/storm-crawler-elasticsearch-*-SNAPSHOT.jar com.digitalpebble.storm.crawler.elasticsearch.ESSeedInjector . seeds.txt -local -conf es-conf.yaml -ttl 60`

The injection topology will terminate by itself after 60 seconds. 

You should then be able to see the seeds in the [status index](http://localhost:9200/status/_search?pretty).

Of course if you have only one seed URL, it would be faster to add it to the _status_ index with CURL as shown above, however if you are planning to add many seeds then using the topology is probably easier. Another situation when you should use the injection topology is when you want shard the URLs per host or domain ('es.status.routing: true').

In [Kibana](http://localhost:5601/#/settings/objects), do `Settings > Objects > Import` and select the file `kibana.json`.  Then go to `DashBoard`, click on `Loads Saved Dashboard` and select `Crawl Status`. You should see a table containing a single line _DISCOVERED 1_.

You are almost ready to launch the crawl. First you'll need to create a _crawl-conf.yaml_ configuration file. The [example conf in core](https://github.com/DigitalPebble/storm-crawler/blob/master/core/crawler-conf.yaml) should be a good starting point. Since we are about to deal with sitemap files, it would be a good idea to add at least 

```
sitemap.sniffContent: true
http.content.limit: -1
```

so that the parser for the sitemap files detects them automatically and that the fetcher does not trim the content, which might cause the parser to fail. As a general good practice, you should also specify the _http.agent.*_ configurations so that the servers you fetch from can identify you.

When it's done run 

`storm jar target/storm-crawler-elasticsearch-*-SNAPSHOT.jar com.digitalpebble.storm.crawler.elasticsearch.ESCrawlTopology -local -conf es-conf.yaml -conf crawl-conf.yaml`
  
to start the crawl. You can remove `-local` to run the topology on a Storm cluster.

The [Metrics dashboard](http://localhost:5601/#/dashboard/Crawl-metrics) in Kibana can be used to monitor the progress of the crawl.







