ESHOST=${1:-"http://localhost:9200"}
ESCREDENTIALS=${2:-"-u elastic:passwordhere"}

# deletes and recreates a status index with a bespoke schema

curl $ESCREDENTIALS -s -XDELETE "$ESHOST/status/" >  /dev/null

echo "Deleted status index"

# http://localhost:9200/status/_mapping/status?pretty

echo "Creating status index with mapping"

curl $ESCREDENTIALS -s -XPUT $ESHOST/status -H 'Content-Type: application/json' -d '
{
	"settings": {
		"index": {
			"number_of_shards": 10,
			"number_of_replicas": 1,
			"refresh_interval": "5s"
		}
	},
	"mappings": {
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
			"properties": {
				"key": {
					"type": "keyword",
					"index": true
				},
				"nextFetchDate": {
					"type": "date",
					"format": "date_optional_time"
				},
				"status": {
					"type": "keyword"
				},
				"url": {
					"type": "keyword"
				}
			}
	}
}'

# deletes and recreates a status index with a bespoke schema

curl $ESCREDENTIALS -s -XDELETE "$ESHOST/metrics*/" >  /dev/null

echo ""
echo "Deleted metrics index"

curl $ESCREDENTIALS -s -XPUT $ESHOST/_ilm/policy/7d-deletion_policy -H 'Content-Type:application/json' -d '
{
    "policy": {
        "phases": {
            "delete": {
                "min_age": "7d",
                "actions": {
                    "delete": {}
                }
            }
        }
    }
}
'

echo "Creating metrics index with mapping"

# http://localhost:9200/metrics/_mapping/status?pretty
curl $ESCREDENTIALS -s -XPOST $ESHOST/_template/storm-metrics-template -H 'Content-Type: application/json' -d '
{
  "index_patterns": "metrics*",
  "settings": {
    "index": {
      "number_of_shards": 1,
      "refresh_interval": "30s"
    },
    "number_of_replicas": 0,
    "lifecycle.name": "7d-deletion_policy"
  },
  "mappings": {
      "_source":         { "enabled": true },
      "properties": {
          "name": {
            "type": "keyword"
          },
          "stormId": {
            "type": "keyword"
          },
          "srcComponentId": {
            "type": "keyword"
          },
          "srcTaskId": {
            "type": "short"
          },
          "srcWorkerHost": {
            "type": "keyword"
          },
          "srcWorkerPort": {
            "type": "integer"
          },
          "timestamp": {
            "type": "date",
            "format": "date_optional_time"
          },
          "value": {
            "type": "double"
          }
      }
  }
}'

# deletes and recreates a doc index with a bespoke schema

curl $ESCREDENTIALS -s -XDELETE "$ESHOST/content*/" >  /dev/null

echo ""
echo "Deleted content index"

echo "Creating content index with mapping"

curl $ESCREDENTIALS -s -XPUT $ESHOST/content -H 'Content-Type: application/json' -d '
{
	"settings": {
		"index": {
			"number_of_shards": 5,
			"number_of_replicas": 1,
			"refresh_interval": "60s"
		}
	},
	"mappings": {
			"_source": {
				"enabled": true
			},
			"properties": {
				"content": {
					"type": "text"
				},
				"description": {
					"type": "text"
				},
				"domain": {
					"type": "keyword"
				},
				"format": {
					"type": "keyword"
				},
				"keywords": {
					"type": "keyword"
				},
				"host": {
					"type": "keyword"
				},
				"title": {
					"type": "text"
				},
				"url": {
					"type": "keyword"
				}
			}
	}
}'

