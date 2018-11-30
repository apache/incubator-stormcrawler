storm-crawler-elasticsearch
===========================

A collection of resources for [Elasticsearch](https://www.elastic.co/products/elasticsearch):
* [IndexerBolt](https://github.com/DigitalPebble/storm-crawler/blob/master/external/elasticsearch/src/main/java/com/digitalpebble/stormcrawler/elasticsearch/bolt/IndexerBolt.java) for indexing documents fetched with StormCrawler
* [Spout](https://github.com/DigitalPebble/storm-crawler/blob/master/external/elasticsearch/src/main/java/com/digitalpebble/stormcrawler/elasticsearch/persistence/ElasticSearchSpout.java) and [StatusUpdaterBolt](https://github.com/DigitalPebble/storm-crawler/blob/master/external/elasticsearch/src/main/java/com/digitalpebble/stormcrawler/elasticsearch/persistence/StatusUpdaterBolt.java) for persisting URL information in recursive crawls
* [MetricsConsumer](https://github.com/DigitalPebble/storm-crawler/blob/master/external/elasticsearch/src/main/java/com/digitalpebble/stormcrawler/elasticsearch/metrics/MetricsConsumer.java)
* [StatusMetricsBolt](https://github.com/DigitalPebble/storm-crawler/blob/master/external/elasticsearch/src/main/java/com/digitalpebble/stormcrawler/elasticsearch/metrics/StatusMetricsBolt.java) for sending the breakdown of URLs per status as metrics and display its evolution over time.

as well as examples of crawl and injection topologies.

We also have resources for [Kibana](https://www.elastic.co/products/kibana) to build basic real-time monitoring dashboards for the crawls, such as the one below.

![bla](https://pbs.twimg.com/media/CR1-waVWEAAh0u4.png)

A dashboard for [Grafana](http://grafana.com/) is available from https://grafana.com/dashboards/2363.

Video tutorial
---------------------

[![Video tutorial](https://i.ytimg.com/vi/KTerugU12TY/hqdefault.jpg)](https://www.youtube.com/watch?v=KTerugU12TY)


Warning
---------------------
If you are running StormCrawler in distributed mode on a Storm 1.0.3 cluster or below, you'll need to upgrade the log4j and slf4j dependencies (see [STORM-2326](https://issues.apache.org/jira/browse/STORM-1386)). This isn't necessary in the more recent releases of Apache Storm.

Also with Elasticsearch 5.x, we now have to specify the following for the Maven Shade configuration\:

```xml
<manifestEntries>
 <Change></Change>
 <Build-Date></Build-Date>
</manifestEntries>
```

Getting started
---------------------

We'll assume that Elasticsearch and Kibana are installed and running on your machine. You'll also need Java, Maven and Storm installed.

With a basic project set up, such as the one generated from the archetype (see main README for instructions), copy the es-conf.yaml and flux files to the directory.

You must then edit the pom.xml and add the dependency for the Elasticsearch module:

```xml
		<dependency>
			<groupId>com.digitalpebble.stormcrawler</groupId>
			<artifactId>storm-crawler-elasticsearch</artifactId>
			<version>${stormcrawler.version}</version>
		</dependency>
```

Then we run the script `ES_IndexInit.sh`, which creates 3 indices : one for persisting the status of URLs (_status_), a template mapping for persisting the Storm metrics (for any indices with a name matching _metrics*_) as well as a third index (_content_) for searching the documents fetched by StormCrawler (you should probably tune its mapping later on e.g. if you want to store the _content_ field). You will also need to edit the script if Elasticsearch is running on a different machine.

We can inject the seed URLs into the _status_ index by putting them in a text file with one URL per line and any keay values separated by tabulations e.g.

`echo 'http://www.theguardian.com/newssitemap.xml	isSitemap=true' > seeds.txt`

Edit the *-conf.yaml files as you see fit, as a general good practice, you should also specify the _http.agent.*_ configurations so that the servers you fetch from can identify you.

Then compile with `mvn clean package` and inject the seeds with \:

`storm jar target/*-1.0-SNAPSHOT.jar  org.apache.storm.flux.Flux --sleep 30000 --local es-injector.flux`

The topology should terminate after 30 seconds, you should then be able to see the seeds in the [status index](http://localhost:9200/status/_search?pretty).

When it's done run 

`storm jar target/*-1.0-SNAPSHOT.jar  org.apache.storm.flux.Flux --local es-crawler.flux`
  
to start the crawl. You can replace `--local` with `--remote` to run the topology on a Storm cluster.

Kibana
---------------------

In [Kibana](http://localhost:5601/#/settings/objects),

1. create the Index Patterns `status` and `metrics`: `Settings > Indices > Add New`, enter `status` as `Index name or pattern`, and press `Create`. Repeat these steps also for `metrics`.
2. to upload the dashboard configurations do `Settings > Objects > Import` and select the file `kibana/status.json`.  Then go to `Dashboard`, click on `Loads Saved Dashboard` and select `Crawl Status`. You should see a table containing a single line _DISCOVERED 1_.
3. repeat the operation with the file `kibana/metrics.json`.

The [Metrics dashboard](http://localhost:5601/#/dashboard/Crawl-metrics) in Kibana can be used to monitor the progress of the crawl.

#### Per time period metric indices (optional)
Note, a second option for the _metrics_ index is available: the use of per time period indices. This best practice is [discussed on the Elastic website](https://www.elastic.co/guide/en/elasticsearch/guide/current/time-based.html).

The crawler config YAML must be updated to use either the day or month Elasticsearch metrics consumer, as shown below with the per day indices consumer:
```
 #Metrics consumers:
    topology.metrics.consumer.register:
       - class: "org.apache.storm.metric.LoggingMetricsConsumer"
         parallelism.hint: 1
       - class: "com.digitalpebble.stormcrawler.elasticsearch.metrics.IndexPerDayMetricsConsumer"
         parallelism.hint: 1
```








