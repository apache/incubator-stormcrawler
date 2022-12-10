#!/bin/bash

OSHOST=${1:-"http://localhost:9200"}
OSCREDENTIALS=${2:-"-u opensearch:passwordhere"}

for name in status content
do
  curl $OSCREDENTIALS -s -XDELETE "$OSHOST/$name/" >  /dev/null
  echo "Deleted '$name' index, now recreating it..."
  curl $OSCREDENTIALS -s -XPUT "$OSHOST/$name" -H 'Content-Type: application/json' --upload-file src/main/resources/$name.mapping
  echo ""
done

### metrics

curl $OSCREDENTIALS -s -XDELETE "$OSHOST/metrics*/" >  /dev/null

echo "Deleted 'metrics' index, now recreating it..."

# http://localhost:9200/metrics/_mapping/status?pretty
curl $OSCREDENTIALS -s -XPOST "$OSHOST/_template/metrics-template" -H 'Content-Type: application/json' --upload-file src/main/resources/metrics.mapping

echo ""
