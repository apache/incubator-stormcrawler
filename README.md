storm-crawler
=============

A collection of resources for building low-latency, large scale web crawlers on [Storm](http://storm.incubator.apache.org/) available under Apache License.

Available from Maven Central with : 

```
<dependency>
    <groupId>com.digitalpebble</groupId>
    <artifactId>storm-crawler</artifactId>
    <version>0.1</version>
</dependency>
```

Alternatively install Maven and do : mvn clean package to generate the full jar then with Storm installed run : 

`storm jar storm-crawler-0.2-SNAPSHOT-jar-with-dependencies.jar com.digitalpebble.storm.crawler.CrawlTopology -conf fetcher-conf.yaml -local`

Mailing list : http://groups.google.com/group/digitalpebble
