[![storm-crawler](http://digitalpebble.com/storm-crawler/img/Logo-small.jpg)](http://digitalpebble.github.io/storm-crawler)
=============

A collection of resources for building low-latency, scalable web crawlers on [Apache Storm](http://storm.apache.org/) available under Apache License.

## How to use
### As a Maven dependency
Available from Maven Central with : 

```
<dependency>
    <groupId>com.digitalpebble</groupId>
    <artifactId>storm-crawler-core</artifactId>
    <version>0.7</version>
</dependency>
```
### Running in local mode
To get started with storm-crawler, it's recommended that you run the CrawlTopology in local mode.
 
NOTE: These instructions assume that you have Maven installed.

First, clone the project from github:
 
 ``` sh
 git clone https://github.com/DigitalPebble/storm-crawler
 ```
 
Then :
``` sh
cd core
mvn clean compile exec:java -Dstorm.topology=com.digitalpebble.storm.crawler.CrawlTopology -Dexec.args="-conf crawler-conf.yaml -local"
```
to run the demo CrawlTopology in local mode.

If you want to run a recursive version, just use this command:

``` sh
cd core
mvn clean compile exec:java -Dstorm.topology=com.digitalpebble.storm.crawler.RecursiveCrawlTopology -Dexec.args="-conf crawler-recursive-conf.yaml -local"
```

### On a Storm cluster
Alternatively, generate an uberjar:
``` sh
mvn clean package
```

and then submit the topology with `storm jar`:

``` sh
storm jar target/storm-crawler-core-0.8-SNAPSHOT.jar  com.digitalpebble.storm.crawler.CrawlTopology -conf crawler-conf.yaml
```

to run it in distributed mode.

If you want to run a recursive version, just use this command:


``` sh
storm jar target/storm-crawler-core-0.8-SNAPSHOT.jar  com.digitalpebble.storm.crawler.RecursiveCrawlTopology -conf crawler-recursive-conf.yaml
```

## Getting help

Mailing list : [http://groups.google.com/group/digitalpebble]

Or use the tag [storm-crawler](http://stackoverflow.com/questions/tagged/storm-crawler) on stackoverflow. 

[DigitalPebble Ltd](http://digitalpebble.com) provide commercial support and consulting for Storm-Crawler.

## Thanks

![alt tag](https://www.yourkit.com/images/yklogo.png)

YourKit supports open source projects with its full-featured Java Profiler.
YourKit, LLC is the creator of <a href="https://www.yourkit.com/java/profiler/index.jsp">YourKit Java Profiler</a>
and <a href="https://www.yourkit.com/.net/profiler/index.jsp">YourKit .NET Profiler</a>,
innovative and intelligent tools for profiling Java and .NET applications.
