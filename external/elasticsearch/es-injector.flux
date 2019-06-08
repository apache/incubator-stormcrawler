name: "injector"

includes:
    - resource: true
      file: "/crawler-default.yaml"
      override: false

    - resource: false
      file: "crawler-conf.yaml"
      override: true

    - resource: false
      file: "es-conf.yaml"
      override: true

spouts:
  - id: "spout"
    className: "com.digitalpebble.stormcrawler.spout.FileSpout"
    parallelism: 1
    constructorArgs:
      - "."
      - "seeds.txt"
      - true

bolts:
# comment in to filter injected URLs
#  - id: "filter"
#    className: "com.digitalpebble.stormcrawler.bolt.URLFilterBolt"
#    parallelism: 1
  - id: "status"
    className: "com.digitalpebble.stormcrawler.elasticsearch.persistence.StatusUpdaterBolt"
    parallelism: 1

streams:
# to filter injected URLs: comment in and connect "filter" and "status" bolts
#  - from: "spout"
#    to: "filter"
#    grouping:
#      type: FIELDS
#      args: ["url"]
#      streamId: "status"
#  - from: "filter"
  - from: "spout"
    to: "status"
    grouping:
      type: CUSTOM
      customClass:
        className: "com.digitalpebble.stormcrawler.util.URLStreamGrouping"
        constructorArgs:
          - "byHost"
      streamId: "status"
