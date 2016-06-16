name: "crawler"

includes:
    - resource: false
      file: "crawler-conf.yaml"
      override: false

spouts:
  - id: "spout"
    className: "com.digitalpebble.storm.crawler.spout.MemorySpout"
    parallelism: 1
    constructorArgs:
      - ["http://www.lequipe.fr/", "http://www.lemonde.fr/", "http://www.bbc.co.uk/", "http://storm.apache.org/", "http://digitalpebble.com/"]

bolts:
  - id: "partitioner"
    className: "com.digitalpebble.storm.crawler.bolt.URLPartitionerBolt"
    parallelism: 1
  - id: "fetcher"
    className: "com.digitalpebble.storm.crawler.bolt.FetcherBolt"
    parallelism: 1
  - id: "sitemap"
    className: "com.digitalpebble.storm.crawler.bolt.SiteMapParserBolt"
    parallelism: 1
  - id: "parse"
    className: "com.digitalpebble.storm.crawler.bolt.JSoupParserBolt"
    parallelism: 1
  - id: "index"
    className: "com.digitalpebble.storm.crawler.indexing.StdOutIndexer"
    parallelism: 1
  - id: "status"
    className: "com.digitalpebble.storm.crawler.persistence.StdOutStatusUpdater"
    parallelism: 1

streams:
  - from: "spout"
    to: "partitioner"
    grouping:
      type: SHUFFLE

  - from: "partitioner"
    to: "fetcher"
    grouping:
      type: FIELDS
      args: ["key"]

  - from: "fetcher"
    to: "sitemap"
    grouping:
      type: LOCAL_OR_SHUFFLE

  - from: "sitemap"
    to: "parse"
    grouping:
      type: LOCAL_OR_SHUFFLE

  - from: "parse"
    to: "index"
    grouping:
      type: LOCAL_OR_SHUFFLE

  - from: "fetch"
    to: "status"
    grouping:
      type: LOCAL_OR_SHUFFLE
      streamId: "status"

  - from: "sitemap"
    to: "status"
    grouping:
      type: LOCAL_OR_SHUFFLE
      streamId: "status"

  - from: "parse"
    to: "status"
    grouping:
      type: LOCAL_OR_SHUFFLE
      streamId: "status"

  - from: "index"
    to: "status"
    grouping:
      type: LOCAL_OR_SHUFFLE
      streamId: "status"

