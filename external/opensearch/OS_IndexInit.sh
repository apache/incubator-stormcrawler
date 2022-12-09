OSHOST=${1:-"http://localhost:9200"}
OSCREDENTIALS=${2:-"-u opensearch:passwordhere"}

curl $OSCREDENTIALS -s -XDELETE "$OSHOST/status/" >  /dev/null

echo "Deleted status index"

echo "Re-creating mapping for status index"

curl $OSCREDENTIALS -s -XPUT $OSHOST/status -H 'Content-Type: application/json' --upload-file src/main/resources/status.mapping

curl $OSCREDENTIALS -s -XDELETE "$OSHOST/content*/" >  /dev/null

echo ""
echo "Deleted content index"

echo "Re-creating mapping for content index "

curl $OSCREDENTIALS -s -XPUT $OSHOST/content -H 'Content-Type: application/json' --upload-file src/main/resources/content.mapping

curl $OSCREDENTIALS -s -XDELETE "$OSHOST/metrics*/" >  /dev/null

echo ""
echo "Deleted metrics index"

echo "Re-creating metrics template"

# http://localhost:9200/metrics/_mapping/status?pretty
curl $OSCREDENTIALS -s -XPOST $OSHOST/_template/metrics-template -H 'Content-Type: application/json' --upload-file src/main/resources/metrics.mapping
