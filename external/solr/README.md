storm-crawler-solr
==================

Set of Solr resources for StormCrawler that allows you to create topologies that consume from a Solr collection and store metrics, status or parsed content into Solr.

## How to use

In your project you can use this by adding the following dependency:

```xml
<dependency>
    <groupId>com.digitalpebble.stormcrawler</groupId>
    <artifactId>storm-crawler-solr</artifactId>
    <version>${stormcrawler.version}</version>
</dependency>
```

## Available resources

* `IndexerBolt`: Implementation of `AbstractIndexerBolt` that allows to index the parsed data and metadata into a specified Solr collection.

* `MetricsConsumer`: Class that allows to store Storm metrics in Solr.

* `SolrSpout`: Spout that allows to get URLs from a specified Solr collection.

* `StatusUpdaterBolt`: Implementation of `AbstractStatusUpdaterBolt` that allows to store the status of each URL along with the serialized metadata in Solr.

* `SolrCrawlTopology`: Example implementation of a topology that use the provided classes, this is intended as an example or a guide on how to use this resources.

* `SeedInjector`: Topology that allow to read URLs from a specified file and store the URLs in a Solr collection using the `StatusUpdaterBolt`. This can be used as a starting point to inject URLs into Solr.

## Configuration options

The available configuration options can be found in the [`solr-conf.yaml`](solr-conf.yaml) file.

For configuring the connection with the Solr server, the following parameters are available: `solr.TYPE.url`, `solr.TYPE.zkhost`, `solr.TYPE.collection`.

> In the previous example `TYPE` can be one of the following values:

> * `indexer`: To reference the configuration parameters of the `IndexerBolt` class.
> * `status`: To reference the configuration parameters of the `SolrSpout` and `StatusUpdaterBolt` classes.
> * `metrics`: To reference the configuration parameters of the `MetricsConsumer` class.

> *Note: Some of this classes provide additional parameter configurations.*

### General parameters

* `solr.TYPE.url`: The URL of the Solr server including the name of the collection that you want to use.

## Additional configuration options

#### MetricsConsumer

In the case of the `MetricsConsumer` class a couple of additional configuration parameters are provided to use the [Document Expiration](https://lucidworks.com/blog/document-expiration/) feature available in Solr since version 4.8.

* `solr.metrics.ttl`: [Date expression](https://cwiki.apache.org/confluence/display/solr/Working+with+Dates) to specify when the document should expire.
* `solr.metrics.ttl.field`: Field to be used to specify the [date expression](https://cwiki.apache.org/confluence/display/solr/Working+with+Dates) that defines when the document should expire.

*Note: The date expression specified in the `solr.metrics.ttl` parameter is not validated. To use this feature some changes in the Solr configuration must be done.*

#### SolrSpout

For the `SolrSpout` class a couple of additional configuration parameters are available to guarantee some *diversity* in the URLs fetched from Solr, in the case that you want to have better coverage of your URLs. This is done using the [collapse and expand](https://cwiki.apache.org/confluence/display/solr/Collapse+and+Expand+Results) feature available in Solr.

* `solr.status.bucket.field`: Field to be used to collapse the documents.
* `solr.status.bucket.maxsize`: Amount of documents to return for each *bucket*.

For instance if you are crawling URLs from different domains, perhaps is of your interest to *balance* the amount of URLs to be processed from each domain, instead of crawling all the available URLs from one domain and then the other.

For this scenario you'll want to collapse on the `host` field (that already is indexed by the `StatusUpdaterBolt`) and perhaps you just want to crawl 100 URLs per domain. For this case is enough to add this to your configuration:

```yaml
solr.status.bucket.field: host
solr.status.bucket.maxsize: 100
```

This feature can be combined with the [partition features](https://github.com/DigitalPebble/storm-crawler/wiki/Configuration#fetching-and-partitioning) provided by storm-crawler to balance the crawling process and not just the URL coverage.

### Metadata

The metadata associated with each URL is also persisted in the Solr collection configured. By default the metadata is stored as separated fields in the collection using a prefix that can be configured using the `solr.status.metadata.prefix` option. If no value is supplied for this option the `metadata` value is used. Take a look at the following example record:

```json
{
  "url": "http://test.com",
  "host": "test.com",
  "status": "DISCOVERED",
  "metadata.url.path": "http://test.com",
  "metadata.depth": "1",
  "nextFetchDate": "2015-10-30T17:26:34.386Z"
}
```

In the previous example the `metadata.url.path` and `metadata.depth` attributes are elements taken from the `metadata` object. If the `SolrSpout` class is used to fetch URLs from Solr, the configured prefix (`metadata.` in this case) is stripped before populating the `Metadata` instance.

## Using SolrCloud

To use a SolrCloud cluster instead of a single Solr server, you must use the following configuration parameters **instead** of the `solr.TYPE.url`:

* `solr.TYPE.zkhost`: URL of the Zookeeper host that holds the information regarding the SolrCloud cluster.

* `solr.TYPE.collection`: Name of the collection that you wish to use.

## Solr configuration

An example collection configuration for each type of data is also provided in the [`cores`](cores) directory. The configuration is very basic but it will allow you to view all the stored data in Solr.

The configuration is only useful as a testing resource, mainly because everything is stored as a `Solr.StrField` which is not very useful for search purposes. Numeric values and dates are also **stored as strings** using dynamic fields.

In the `metrics` collection an `id` field is configured to be populated with an auto-generated UUID for each document, this configuration is placed in the `solrconfig.xml` file. The `id` field will be used as the `uniqueKey`.

In the `parse` and `status` cores the `uniqueKey` is defined to be the `url` field.

Also keep in mind that depending on your needs you can use the [Schemaless Mode](https://cwiki.apache.org/confluence/display/solr/Schemaless+Mode) available in Solr.

To start SOLR with the preconfigured cores for StormCrawler, you can do `bin/solr start -s storm-crawler/external/solr/cores`, then open the SOLR UI (http://localhost:8983) to check that they have been loaded correctly. Alternatively, create the cores (here `status`) by `bin/solr create -c status -d storm-crawler/external/solr/cores/status/`.

