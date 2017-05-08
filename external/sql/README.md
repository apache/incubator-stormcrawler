SQL module for StormCrawler

Based on MySQL, see [tableCreation.script](https://github.com/DigitalPebble/storm-crawler/blob/master/external/sql/tableCreation.script) for the creation of the tables. Contains a spout implementation as well as a status updater bolt.
This [tutorial](https://digitalpebble.blogspot.co.uk/2015/09/index-web-with-aws-cloudsearch.html) uses this module.
You can either inject the seeds directly as done in the tutorial or reuse the injection topology from the elasticsearch module and specify the statusupdaterbolt from the mysql module instead. Another approach could be to simply to add a MemorySpout to the topology alongside the SQLSpout.
