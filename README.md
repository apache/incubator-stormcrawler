storm-crawler
=============

A collection of resources for building low-latency, large scale web crawlers on Storm (and maybe HBase).

Install Maven and call : mvn clean assembly:assembly to generate the full jar 

storm jar target/weborama-fetcher-0.1-SNAPSHOT-jar-with-dependencies.jar com.weborama.fetcher.FetcherTopology -conf fetcher-conf.yaml -local

Mailing list : http://groups.google.com/group/digitalpebble
