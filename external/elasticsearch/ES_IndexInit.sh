# deletes and recreates a status index with a bespoke schema

curl -XDELETE 'http://localhost:9200/status/'

echo ""
echo "Deleted status index"

# http://localhost:9200/status/_mapping/status?pretty

curl -XPOST localhost:9200/status -d '
{
  "mappings": {
    "status": {
      "dynamic_templates": [
        {
          "metadata": {
            "path_match": "metadata.*",
            "match_mapping_type": "string",
            "mapping": {
              "type": "string",
              "index": "not_analyzed"
            }
          }
        }
      ],
      "_source": {
        "enabled": true
      },
      "_id": {
        "path": "url"
      },
      "properties": {
        "nextFetchDate": {
          "type": "date",
          "format": "dateOptionalTime"
        },
        "status": {
          "type": "string",
          "index": "not_analyzed",
          "store": true
        },
        "url": {
          "type": "string",
          "index": "not_analyzed",
          "store": true
        }
      }
    }
  }
}'

echo ""
echo "Created status index with mapping"

now=`date -Iseconds`

curl -XPOST 'http://localhost:9200/status/status/' -d '{
  "url": "http:\/\/www.theguardian.com\/newssitemap.xml",
  "status": "DISCOVERED",
  "nextFetchDate": "'$now'",
  "metadata": {
    "isSitemap": "true"
  }
}'

echo ""
echo "Sent seed URL"

# deletes and recreates a status index with a bespoke schema

curl -XDELETE 'http://localhost:9200/metrics/'

echo ""
echo "Deleted metrics index"

# http://localhost:9200/metrics/_mapping/status?pretty

curl -XPOST localhost:9200/metrics -d '
{
  "mappings": {
    "datapoint": {
      "_ttl" : { "enabled" : true, "default" : "1d" },
      "properties": {
        "name": {
          "type": "string"
        },
        "srcComponentId": {
          "type": "string",
          "index": "not_analyzed"
        },
        "srcTaskId": {
          "type": "long"
        },
        "srcWorkerHost": {
          "type": "string",
          "index": "not_analyzed"
        },
        "srcWorkerPort": {
          "type": "long"
        },
        "timestamp": {
          "type": "date",
          "format": "dateOptionalTime"
        },
        "value": {
          "type": "double"
        }
      }
    }
  }
}'

echo ""
echo "Created metrics index with mapping"

