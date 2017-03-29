# deletes and recreates a status index with a bespoke schema

curl -s -XDELETE 'http://localhost:9200/status/' >  /dev/null

echo "Deleted status index"

# http://localhost:9200/status/_mapping/status?pretty

echo "Creating status index with mapping"

curl -s -XPOST localhost:9200/status -d '
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
						"type": "string",
						"index": "not_analyzed"
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
					"type": "string",
					"index": "not_analyzed"
				},
				"url": {
					"type": "string",
					"index": "not_analyzed"
				}
			}
		}
	}
}'

# deletes and recreates a status index with a bespoke schema

curl -s -XDELETE 'http://localhost:9200/metrics*/' >  /dev/null

echo ""
echo "Deleted metrics index"

echo "Creating metrics index with mapping"

# http://localhost:9200/metrics/_mapping/status?pretty
curl -s -XPOST localhost:9200/_template/storm-metrics-template -d '
{
  "template": "metrics*",
  "settings": {
    "index": {
      "number_of_shards": 1,
      "refresh_interval": "30s"
    },
    "number_of_replicas" : 0
  },
  "mappings": {
    "datapoint": {
      "_all":            { "enabled": false },
      "_source":         { "enabled": true },
      "properties": {
          "name": {
            "type": "string",
            "index": "not_analyzed"
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

# deletes and recreates a status index with a bespoke schema

curl -s -XDELETE 'http://localhost:9200/index/' >  /dev/null

echo ""
echo "Deleted docs index"

echo "Creating docs index with mapping"

curl -s -XPOST localhost:9200/index -d '
{
	"settings": {
		"index": {
			"number_of_shards": 5,
			"number_of_replicas": 1,
			"refresh_interval": "60s"
		}
	},
	"mappings": {
		"doc": {
			"_source": {
				"enabled": false
			},
			"_all": {
				"enabled": false
			},
			"properties": {
				"content": {
					"type": "string",
					"index": "analyzed"
				},
				"host": {
					"type": "string",
					"index": "not_analyzed",
					"store": true
				},
				"title": {
					"type": "string",
					"index": "analyzed",
					"store": true
				},
				"url": {
					"type": "string",
					"index": "no",
					"store": true
				}
			}
		}
	}
}'

