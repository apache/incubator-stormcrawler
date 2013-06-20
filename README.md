storm-crawler
=============

Low-latency, large scale crawler based on Storm (and maybe HBase). Merely a field for experimentation for now but hopefully we'll have some Spouts and Bolts that will be useful for building crawlers.

Install Maven and call : mvn clean assembly:assembly to generate the full jar 

storm jar storm-crawler-0.1-SNAPSHOT-jar-with-dependencies.jar com.digitalpebble.storm.crawler.CrawlTopology crawl

Mailing list : http://groups.google.com/group/digitalpebble
