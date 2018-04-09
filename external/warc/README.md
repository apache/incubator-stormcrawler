#  Resources for generating WARC files with StormCrawler

First, you need to add the WARC module to the dependencies of your project.

```
		<dependency>
			<groupId>com.digitalpebble.stormcrawler</groupId>
			<artifactId>storm-crawler-warc</artifactId>
			<version>${storm-crawler.version}</version>
		</dependency>
```

Include the following snippet in your crawl topology

```java 
        String warcFilePath = "/warc";

        FileNameFormat fileNameFormat = new WARCFileNameFormat()
                .withPath(warcFilePath);

        Map<String,String> fields = new HashMap<>();
        fields.put("software:", "StormCrawler 1.0 http://stormcrawler.net/");
        fields.put("conformsTo:", "http://www.archive.org/documents/WarcFileFormat-1.0.html");
        
        WARCHdfsBolt warcbolt = (WARCHdfsBolt) new WARCHdfsBolt()
                .withFileNameFormat(fileNameFormat);
        warcbolt.withHeader(fields);

        // can specify the filesystem - will use the local FS by default
        String fsURL = "hdfs://localhost:9000";
        warcbolt.withFsUrl(fsURL);
        
        // a custom max length can be specified - 1 GB will be used as a default
        FileSizeRotationPolicy rotpol = new FileSizeRotationPolicy(50.0f,
                Units.MB);
        warcbolt.withRotationPolicy(rotpol);

        builder.setBolt("warc", warcbolt).localOrShuffleGrouping("fetch");
```

If you use Flux, you could add it like so:

```
components:
  - id: "WARCFileNameFormat"
    className: "com.digitalpebble.stormcrawler.warc.WARCFileNameFormat"
    configMethods:
      - name: "withPath"
        args:
          - "/warc"

  - id: "rotationPolicy"
    className: "org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy"
    constructorArgs:
      - 50.0
      - MB

[...]

 bolts:
 - id: "warc"
    className: "com.digitalpebble.stormcrawler.warc.WARCHdfsBolt"
    parallelism: 1
    configMethods:
      - name: "withFileNameFormat"
        args:
          - ref: "WARCFileNameFormat"
      - name: "withRotationPolicy"
        args:
          - ref: "rotationPolicy"

```

Each instance of the bolt will generate a WARC file and close it once it has reached the required size.

Please note that the WARCHdfsBolt will automatically ack tuples - regardless of whether the writing operation was successful. The bolt is also a dead-end and does not output tuples to subsequent bolts.

