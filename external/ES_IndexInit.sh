# deletes and recreates a status index with a bespoke schema

curl -XDELETE 'http://localhost:9200/status/'

echo ""
echo "Deleted status index"

# http://localhost:9200/status/_mapping/status?pretty

curl -XPOST localhost:9200/status -d '
{
  "mappings": {
    "status": {
      "_source": {
        "enabled": true
      },
      "_id": {
        "path": "url"
      },
      "properties": {
        "metadata": {
          "type": "string",
          "index": "not_analyzed",
          "store": true
        },
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
    "url" : "http://www.theguardian.com/newssitemap.xml",
    "status" : "DISCOVERED",
    "nextFetchDate" : "'$now'",
    "metadata" : "isSitemap: true"
}'

echo ""
echo "Sent seed URL"
