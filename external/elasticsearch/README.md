stormcrawler-elasticsearch
===========================

A collection of resources for [Elasticsearch](https://www.elastic.co/products/elasticsearch):
* [IndexerBolt](https://github.org/apache/incubator-stormcrawler/blob/master/external/elasticsearch/src/main/java/org/apache/stormcrawler/elasticsearch/bolt/IndexerBolt.java) for indexing documents crawled with StormCrawler
* [Spouts](https://github.org/apache/incubator-stormcrawler/blob/master/external/elasticsearch/src/main/java/org/apache/stormcrawler/elasticsearch/persistence/AggregationSpout.java) and [StatusUpdaterBolt](https://github.org/apache/incubator-stormcrawler/blob/master/external/elasticsearch/src/main/java/org/apache/stormcrawler/elasticsearch/persistence/StatusUpdaterBolt.java) for persisting URL information in recursive crawls
* [MetricsConsumer](https://github.org/apache/incubator-stormcrawler/blob/master/external/elasticsearch/src/main/java/org/apache/stormcrawler/elasticsearch/metrics/MetricsConsumer.java)
* [StatusMetricsBolt](https://github.org/apache/incubator-stormcrawler/blob/master/external/elasticsearch/src/main/java/org/apache/stormcrawler/elasticsearch/metrics/StatusMetricsBolt.java) for sending the breakdown of URLs per status as metrics and display its evolution over time.

as well as an archetype containing a basic crawl topology and its configuration.

We also have resources for [Kibana](https://www.elastic.co/products/kibana) to build basic real-time monitoring dashboards for the crawls. A dashboard for [Grafana](http://grafana.com/) is also [available](https://grafana.com/dashboards/2363).

Getting started
---------------------

Use the archetype for Elasticsearch with:

`mvn archetype:generate -DarchetypeGroupId=org.apache.stormcrawler -DarchetypeArtifactId=stormcrawler-elasticsearch-archetype -DarchetypeVersion=2.11`

You'll be asked to enter a groupId (e.g. com.mycompany.crawler), an artefactId (e.g. stormcrawler), a version, a package name and details about the user agent to use.

This will not only create a fully formed project containing a POM with the dependency above but also a set of resources, configuration files and a topology class. Enter the directory you just created (should be the same as the artefactId you specified earlier) and follow the instructions on the README file.

Video tutorial
---------------------

[![Video tutorial](https://i.ytimg.com/vi/8kpJLPdhvLw/hqdefault.jpg)](https://youtu.be/8kpJLPdhvLw)


Kibana
---------------------

To import the dashboards into a local instance of Kibana, go into the folder _kibana_ and run the script _importKibana.sh_. 

You should see something like 

```
Importing status dashboard into Kibana
{"success":true,"successCount":4}
Importing metrics dashboard into Kibana
{"success":true,"successCount":9}
```

The [dashboard screen](http://localhost:5601/app/kibana#/dashboards) should show both the status and metrics dashboards. If you click on `Crawl Status`, you should see 2 tables containing the count of URLs per status and the top hostnames per URL count.
The [Metrics dashboard](http://localhost:5601/app/kibana#/dashboard/Crawl-metrics) can be used to monitor the progress of the crawl.

The file _storm.ndjson_ is used to display some of Storm's internal metrics and is not added by default.

#### Per time period metric indices (optional)

The _metrics_ index can be configured per tine period. This best practice is [discussed on the Elastic website](https://www.elastic.co/guide/en/elasticsearch/guide/current/time-based.html).

The crawler config YAML must be updated to use an optional argument as shown below to have one index per day:

```
 #Metrics consumers:
    topology.metrics.consumer.register:
         - class: "org.apache.stormcrawler.elasticsearch.metrics.MetricsConsumer"
           parallelism.hint: 1
           argument: "yyyy-MM-dd"
```








