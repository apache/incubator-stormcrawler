#!/bin/sh

BIN=$(dirname $0)

echo "Importing status dashboard into OpenSearch Dashboards"
curl -X POST "localhost:5601/api/saved_objects/_import" -H "osd-xsrf: true" --form file=@$BIN/status.ndjson
echo ""

echo "Importing metrics dashboard into OpenSearch Dashboards"
curl -X POST "localhost:5601/api/saved_objects/_import" -H "osd-xsrf: true" --form file=@$BIN/metrics.ndjson
echo ""

# Storm internal metrics
# curl -X POST "localhost:5601/api/saved_objects/_import" -H "kbn-xsrf: true" --form file=@$BIN/storm.ndjson
