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
