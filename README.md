[![storm-crawler](http://stormcrawler.net/img/Logo-small.jpg)](http://stormcrawler.net/)
=============

[![license](https://img.shields.io/github/license/digitalpebble/storm-crawler.svg?maxAge=2592000?style=plastic)](http://www.apache.org/licenses/LICENSE-2.0)

[![Build Status](https://travis-ci.org/DigitalPebble/storm-crawler.svg?branch=master)](https://travis-ci.org/DigitalPebble/storm-crawler)

StormCrawler is an open source collection of resources for building low-latency, scalable web crawlers on [Apache Storm](http://storm.apache.org/). It is provided under [Apache License](http://www.apache.org/licenses/LICENSE-2.0) and is written mostly in Java.

## Quickstart

NOTE: These instructions assume that you have [Apache Maven](https://maven.apache.org/install.html) installed.

The easiest way to get started is to generate a brand new StormCrawler-based project using \: 

`mvn archetype:generate -DarchetypeGroupId=com.digitalpebble.stormcrawler -DarchetypeArtifactId=storm-crawler-archetype -DarchetypeVersion=1.12.1`

You'll be asked to enter a groupId (e.g. com.mycompany.crawler), an artefactId (e.g. stormcrawler), a version and package name.

This will not only create a fully formed project containing a POM with the dependency above but also the default resource files, a default CrawlTopology class and a configuration file. Enter the directory you just created (should be the same as the artefactId you specified earlier) and follow the instructions on the README file.

Alternatively if you can't or don't want to use the Maven archetype above, you can simply copy the files from [archetype-resources](https://github.com/DigitalPebble/storm-crawler/tree/master/archetype/src/main/resources/archetype-resources).

Have a look at the code of the [CrawlTopology class](https://github.com/DigitalPebble/storm-crawler/blob/master/archetype/src/main/resources/archetype-resources/src/main/java/CrawlTopology.java), the [crawler-conf.yaml](https://github.com/DigitalPebble/storm-crawler/blob/master/archetype/src/main/resources/archetype-resources/crawler-conf.yaml) file as well as the files in [src/main/resources/](https://github.com/DigitalPebble/storm-crawler/tree/master/archetype/src/main/resources/archetype-resources/src/main/resources), they are all that is needed to run a crawl topology : all the other components come from the core module.

## Getting help

The [WIKI](https://github.com/DigitalPebble/storm-crawler/wiki) is a good place to start your investigations but if you are stuck please use the [DigitalPebble mailing list](http://groups.google.com/group/digitalpebble) or use the tag [stormcrawler](http://stackoverflow.com/questions/tagged/stormcrawler) on StackOverflow. 

[DigitalPebble Ltd](http://digitalpebble.com) provide commercial support and consulting for Storm-Crawler.

## Thanks

![alt tag](https://www.yourkit.com/images/yklogo.png)

YourKit supports open source projects with its full-featured Java Profiler.
YourKit, LLC is the creator of <a href="https://www.yourkit.com/java/profiler/index.jsp">YourKit Java Profiler</a>
and <a href="https://www.yourkit.com/.net/profiler/index.jsp">YourKit .NET Profiler</a>,
innovative and intelligent tools for profiling Java and .NET applications.
