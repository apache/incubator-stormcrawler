package com.digitalpebble.storm.crawler;

import com.digitalpebble.storm.crawler.util.Configuration;

public class StormConfiguration extends Configuration {

    /**
     * Create a {@link Configuration} for Storm. This will load the standard
     * Storm resources, <code>storm-default.xml</code> and
     * <code>storm-site.xml</code> overrides.
     */
    public static Configuration create() {
        Configuration conf = new Configuration(false);
        conf.addResource("storm-default.xml");
        conf.addResource("storm-site.xml");
        return conf;
    }

}
