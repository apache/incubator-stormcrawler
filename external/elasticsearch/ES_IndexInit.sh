# deletes and recreates a status index with a bespoke schema

curl -XDELETE 'http://localhost:9200/status/'

echo ""
echo "Deleted status index"

# http://localhost:9200/status/_mapping/status?pretty

curl -XPUT localhost:9200/status -d '
{
	"settings": {
		"index": {
			"number_of_shards": 10,
			"number_of_replicas": 1,
			"refresh_interval": "5s"
		}
	},
	"mappings": {
		"status": {
			"dynamic_templates": [{
				"metadata": {
					"path_match": "metadata.*",
					"match_mapping_type": "string",
					"mapping": {
						"type": "keyword"
					}
				}
			}],
			"_source": {
				"enabled": true
			},
			"_all": {
				"enabled": false
			},
			"properties": {
				"nextFetchDate": {
					"type": "date",
					"format": "dateOptionalTime"
				},
				"status": {
					"type": "keyword"
				},
				"url": {
					"type": "keyword"
				}
			}
		}
	}
}'

# deletes and recreates a status index with a bespoke schema

curl -XDELETE 'http://localhost:9200/metrics/'

echo ""
echo "Deleted metrics index"

# http://localhost:9200/metrics/_mapping/status?pretty
curl -XPOST localhost:9200/_template/storm-metrics-template -d '
{
  "template": "metrics*",
  "settings": {
    "index": {
      "number_of_shards": 1,
      "refresh_interval": "5s"
    },
    "number_of_replicas" : 0
  },
  "mappings": {
    "datapoint": {
      "_all":            { "enabled": false },
      "_source":         { "enabled": true },
      "properties": {
          "name": {
            "type": "keyword"
          },
          "srcComponentId": {
            "type": "keyword"
          },
          "srcTaskId": {
            "type": "long"
          },
          "srcWorkerHost": {
            "type": "keyword"
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

