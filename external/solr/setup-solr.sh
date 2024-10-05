# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash

STATUS_SHARDS=$(grep 'solr.status.routing.shards' solr-conf.yaml | sed -e 's/.*: //' | tr -d ' ')
ROUTER_FIELD=$(grep 'solr.status.routing.fieldname' solr-conf.yaml | sed -e 's/.*: //' | tr -d ' ')

SOLR_PORT=8983
SOLR_HOME=/opt/solr-9.7.0

$SOLR_HOME/bin/solr start -cloud -p $SOLR_PORT

$SOLR_HOME/bin/solr zk upconfig -n "docs" -d configsets/docs -z localhost:9983
$SOLR_HOME/bin/solr zk upconfig -n "status" -d configsets/status -z localhost:9983
$SOLR_HOME/bin/solr zk upconfig -n "metrics" -d configsets/metrics -z localhost:9983

echo "Creating 'docs' collection ..."
curl -X POST "http://localhost:$SOLR_PORT/api/collections" -H "Content-type:application/json" -d '
  {
    "name": "docs",
    "numShards": 1,
    "replicationFactor": 1,
    "config": "docs"
  }'

echo "Creating 'status' collection with $STATUS_SHARDS replicas and routing based on '$ROUTER_FIELD' ..."
curl -X POST "http://localhost:$SOLR_PORT/api/collections" -H "Content-type:application/json" -d '
  {
    "name": "status",
    "numShards": '$STATUS_SHARDS',
    "replicationFactor": 1,
    "router": {
        "name": "compositeId",
        "field": '$ROUTER_FIELD'
    },
    "config": "status"
  }'

echo "Creating 'metrics' collection ..."
curl -X POST "http://localhost:$SOLR_PORT/api/collections" -H "Content-type:application/json" -d '
  {
    "name": "metrics",
    "numShards": 1,
    "replicationFactor": 1,
    "config": "metrics"
  }'
