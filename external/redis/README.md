storm-crawler-redis
===========================

Spout and StatusUpdaterBolt for Redis. The info about a URL is stored with the prefix 's_' e.g. 's_http://stormcrawler.net/' and has the concatenation of the status value + tab + metadata as value. 
We also store the crawl frontier with the prefix 'q_' e.g. 'q_VALUE' where VALUE can be the hostname, domainname or IP depending on the value of the config config _partition.url.mode_.
The value for the 'q_' keys is a list of URLs, with the most recent URLs placed at the end (FIFO).

The initial seed URLs can be pushed to the queues with the Redis RPUSH command (assuming `partition.url.mode` is set to `byDomain`).

```
FLUSHALL
RPUSH q_stormcrawler.net http://stormcrawler.net/
```

Limitations 
- when injecting a large # of URLS needs double the size as URLs are stored individually as s_ as well as sets in the queues q_hostname
- no scheduling : URLs are not refetched including those for which we got a fetch-error status
- URL fetched without metadata
- No sharding yet
- Can lose URLs being processed if the worker running the spout crashes
