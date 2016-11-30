Cassandra module for StormCrawler
=================================

```
CREATE KEYSPACE IF NOT EXISTS stormcrawler WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

DROP TABLE stormcrawler.webpage;

CREATE TABLE stormcrawler.webpage (url text, status text, next_fetch_date timestamp, hostname text, metadata text, PRIMARY KEY (hostname, url));

INSERT INTO stormcrawler.webpage (url, status, next_fetch_date, hostname, metadata) VALUES ('http://www.lemonde.fr', 'DISCOVERED', '2016-11-28 16:18:24', 'www.lemonde.fr', '') IF NOT EXISTS;
```

The injector.flux file can be used as a replacement for the last step. 