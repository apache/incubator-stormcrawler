storm-crawler-elasticsearch
===========================

A collection of resources for [Elasticsearch](https://www.elastic.co/products/elasticsearch):
* IndexerBolt for indexing documents fetched with StormCrawler
* Spout and StatusUpdaterBolt for persisting URL information in recursive crawls
* MetricsConsumer 

as well as examples of crawl and injection topologies.

We also have resources for [Kibana](https://www.elastic.co/products/kibana) to build basic real-time monitoring dashboards for the crawls, such as the one below.

![bla](https://pbs.twimg.com/media/CR1-waVWEAAh0u4.png)  

Quick tutorial
---------------------

We'll assume that Elasticsearch and Kibana are installed and running on your machine. You'll also need Java, Maven and Storm installed.

First compile the code for the ElasticSearch module with `mvn clean install`.

Then we run the script `ES_IndexInit.sh`, which creates 2 indices : one for persisting the status of URLs (_status_) and one for persisting the Storm metrics (_metrics_). A third index (_metrics_) for searching the documents fetched by stormcrawler will be created automatically by the topology, you should probably tune its mapping later on.

We can either inject seed URLs directly into the _status_ index \:

```
now=`date -Iseconds`

curl -XPOST 'http://localhost:9200/status/status/' -d '{
  "url": "http:\/\/www.theguardian.com\/newssitemap.xml",
  "status": "DISCOVERED",
  "nextFetchDate": "'$now'",
  "metadata": {
    "isSitemap": "true"
  }
}'
```

or put the seed in a text file with one URL per line and the key/value metadata separated by tabs e.g.

`echo 'http://www.theguardian.com/newssitemap.xml isSitemap=true' > seeds.txt`

Create a new configuration file _crawl-conf.yaml_ with the following content `metadata.transfer: "isSitemap"`

then call the ESSeedInjector topology with 

`storm jar target/storm-crawler-elasticsearch-0.7-SNAPSHOT.jar com.digitalpebble.storm.crawler.elasticsearch.ESSeedInjector . seeds.txt -local -conf es-conf.yaml -conf crawl-conf.yaml -ttl 60`

You should then be able to see the seeds in the [status index](http://localhost:9200/status/_search?pretty). The injection topology will terminate by itself after 60 seconds.

Of course if you have only one seed URL, it would be faster to add it to the _status_ index with CURL as shown above, however if you are planning to add many seeds then using the topology is probably easier.

In [Kibana](http://localhost:5601/#/settings/objects), do `Settings > Objects > Import` and select the file `kibana.json`.  Then go to `DashBoard`, click on `Loads Saved Dashboard` and select `Crawl Status`. You should see a table containing a single line _DISCOVERED 1_.

You are almost ready to launch the crawl. First you'll need to add more elements to the _crawl-conf.yaml_ configuration file. The [example conf in core](https://github.com/DigitalPebble/storm-crawler/blob/master/core/crawler-conf.yaml) should be a good starting point. When it's done run 

`storm jar target/storm-crawler-elasticsearch-0.7-SNAPSHOT.jar com.digitalpebble.storm.crawler.elasticsearch.ESCrawlTopology -local -conf es-conf.yaml -conf crawl-conf.yaml`
  
to start the crawl. You can remove `-local` to run the topology on a Storm cluster.

The [Metrics dashboard](http://localhost:5601/#/dashboard/Crawl-metrics) in Kibana can be used to monitor the progress of the crawl.







