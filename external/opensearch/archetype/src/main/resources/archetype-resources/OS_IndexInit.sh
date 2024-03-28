#!/bin/bash

OSHOST=${1:-"http://localhost:9200"}
OSCREDENTIALS=${2:-"-u opensearch:passwordhere"}

curl $OSCREDENTIALS -s -XDELETE "$OSHOST/status/" >  /dev/null
echo "Deleted 'status' index, now recreating it..."
curl $OSCREDENTIALS -s -XPUT "$OSHOST/status" -H 'Content-Type: application/json' --upload-file src/main/resources/status.mapping

echo ""

curl $OSCREDENTIALS -s -XDELETE "$OSHOST/content/" >  /dev/null
echo "Deleted 'content' index, now recreating it..."
curl $OSCREDENTIALS -s -XPUT "$OSHOST/content" -H 'Content-Type: application/json' --upload-file src/main/resources/indexer.mapping

### metrics

curl $OSCREDENTIALS -s -XDELETE "$OSHOST/metrics*/" >  /dev/null

echo "Deleted 'metrics' index, now recreating it..."

# http://localhost:9200/metrics/_mapping/status?pretty
curl $OSCREDENTIALS -s -XPOST "$OSHOST/_template/metrics-template" -H 'Content-Type: application/json' --upload-file src/main/resources/metrics.mapping

echo ""
