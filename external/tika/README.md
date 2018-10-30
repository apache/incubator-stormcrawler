# Tika module

Contains a bolt implementation which uses [Apache Tika](http://tika.apache.org/) to parse documents. This bolt can be used as a drop-in replacement for the JSoup-based on from the core module.
To use it alongside the JSoup parser i.e. let JSoup handle HTML content and Tika do everything else, you need to configure the JSoupParser with `jsoup.treat.non.html.as.error: false` so that documents that are not HTML don't get failed but passed on.
The next step is to use a [RedirectionBolt](https://github.com/DigitalPebble/storm-crawler/blob/master/external/tika/src/main/java/com/digitalpebble/stormcrawler/tika/RedirectionBolt.java) to send documents which have not been parsed with Jsoup to Tika on a bespoke stream called `tika`, finally the IndexingBolt and StatusUpdaterBolts need to be connected to the outputs of both `shunt` and `tika` on the default stream. 

```
 builder.setBolt("jsoup", new JSoupParserBolt()).localOrShuffleGrouping(
          "sitemap");
  
  builder.setBolt("shunt", new RedirectionBolt()).localOrShuffleGrouping("jsoup");
  
  builder.setBolt("tika", new ParserBolt()).localOrShuffleGrouping("shunt",
          "tika");
  
  builder.setBolt("indexer", new IndexingBolt(), numWorkers)
          .localOrShuffleGrouping("shunt").localOrShuffleGrouping("tika");
 ```
