name: "injection"

includes:
  - resource: true
    file: "/crawler-default.yaml"
    override: false

  - resource: false
    file: "crawler-conf.yaml"
    override: true

  - resource: false
    file: "solr-conf.yaml"
    override: true

spouts:
  - id: "filespout"
    className: "org.apache.stormcrawler.spout.FileSpout"
    parallelism: 1
    constructorArgs:
      - "."
      - "seeds.txt"
      - true

bolts:
  - id: "filter"
    className: "org.apache.stormcrawler.bolt.URLFilterBolt"
    parallelism: 1

  - id: "status"
    className: "org.apache.stormcrawler.solr.persistence.StatusUpdaterBolt"
    parallelism: 1

streams:
  - from: "filespout"
    to: "filter"
    grouping:
      type: FIELDS
      args: ["url"]
      streamId: "status"

  - from: "filter"
    to: "status"
    grouping:
      streamId: "status"
      type: CUSTOM
      customClass:
        className: "org.apache.stormcrawler.util.URLStreamGrouping"
        constructorArgs:
          - "byDomain"
