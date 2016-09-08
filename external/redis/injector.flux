name: "injector"

includes:
    - resource: true
      file: "/crawler-default.yaml"
      override: false

    - resource: false
      file: "crawler-conf.yaml"
      override: true

components:
  - id: "scheme"
    className: "com.digitalpebble.stormcrawler.util.StringTabScheme"
    constructorArgs:
      - DISCOVERED

spouts:
  - id: "spout"
    className: "com.digitalpebble.stormcrawler.spout.FileSpout"
    parallelism: 1
    constructorArgs:
      - "/data"
      - "*.txt"
      - ref: "scheme"

bolts:
  - id: "status"
    className: "com.digitalpebble.stormcrawler.redis.StatusUpdaterBolt"
    parallelism: 1

streams:
  - from: "spout"
    to: "status"
    grouping:
      type: FIELDS
      args: ["url"]
