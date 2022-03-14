# URLFRONTIER

This module contains Spout and StatusUpdaterBolt implementations to communicate with a [URLFrontier](https://github.com/crawler-commons/url-frontier) service.

## Run the service

The easiest way to run the Frontier is to use Docker and do

```
 docker pull crawlercommons/url-frontier:1.0
 docker run --rm --name frontier -p 7071:7071 crawlercommons/url-frontier:1.0
```

## Configuration


Below are the configuration elements and their default values

```
urlfrontier.host: localhost
urlfrontier.port: 7071

urlfrontier.max.buckets: 10
urlfrontier.max.urls.per.bucket:10
```

Your StormCrawler topology requires the following dependency in its pom.xml (just like with any other module)

```
 <dependency>
  <groupId>com.digitalpebble.stormcrawler</groupId>
  <artifactId>storm-crawler-urlfrontier</artifactId>
  <version>${stormcrawler.version}</version>
 </dependency>
 ```
 
 but can also include
 
 ```
<dependency>
 <groupId>com.github.crawler-commons</groupId>
 <artifactId>urlfrontier-client</artifactId>
 <version>1.2</version>
</dependency>
```

so that the [URLFrontier client](https://github.com/crawler-commons/url-frontier/client) gets added to the uber-jar.

This way you will be able to interact with the Frontier from the command line, e.g. to inject seeds

```
java -cp target/*.jar crawlercommons.urlfrontier.client.Client PutUrls -f seeds.txt
```


