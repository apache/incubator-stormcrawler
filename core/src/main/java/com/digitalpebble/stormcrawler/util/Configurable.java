package com.digitalpebble.stormcrawler.util;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.parse.ParseFilters;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;

public interface Configurable {

    static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ParseFilters.class);

    /**
     * Called when this filter is being initialized
     * 
     * @param stormConf
     *            The Storm configuration used for the ParserBolt
     * @param filterParams
     *            the filter specific configuration. Never null
     */
    public default void configure(Map stormConf, JsonNode filterParams) {
    }

    /**
     * Used by URLFilters and ParseFilters classes to load the configuration
     * from JSON
     **/
    @SuppressWarnings("rawtypes")
    public static Configurable[] configure(Map stormConf, JsonNode filtersConf,
            Class<? extends Configurable> filterClass, String callingClass) {
        // initialises the filters
        List<Configurable> filterLists = new ArrayList<>();

        // get the filters part
        filtersConf = filtersConf.get(callingClass);

        if (filtersConf == null) {
            LOG.info("No field {} in JSON config. Skipping", callingClass);
            return (Configurable[]) Array.newInstance(filterClass, 0);
        }

        // conf node contains a list of objects
        Iterator<JsonNode> filterIter = filtersConf.elements();
        while (filterIter.hasNext()) {
            JsonNode afilterConf = filterIter.next();
            String filterName = "<unnamed>";
            JsonNode nameNode = afilterConf.get("name");
            if (nameNode != null) {
                filterName = nameNode.textValue();
            }
            JsonNode classNode = afilterConf.get("class");
            if (classNode == null) {
                LOG.error("Filter {} doesn't specified a 'class' attribute", filterName);
                continue;
            }
            String className = classNode.textValue().trim();
            filterName += '[' + className + ']';
            // check that it is available and implements the interface
            // ParseFilter
            try {
                Class<?> filterImplClass = Class.forName(className);
                boolean subClassOK = filterClass.isAssignableFrom(filterImplClass);
                if (!subClassOK) {
                    LOG.error("Filter {} does not extend {}", filterName, filterClass.getName());
                    continue;
                }
                Configurable filterInstance = (Configurable) filterImplClass.newInstance();

                JsonNode paramNode = afilterConf.get("params");
                if (paramNode != null) {
                    filterInstance.configure(stormConf, paramNode);
                } else {
                    // Pass in a nullNode if missing
                    filterInstance.configure(stormConf, NullNode.getInstance());
                }

                filterLists.add(filterInstance);
                LOG.info("Setup {}", filterName);
            } catch (Exception e) {
                LOG.error("Can't setup {}: {}", filterName, e);
                throw new RuntimeException("Can't setup " + filterName, e);
            }
        }

        return filterLists.toArray(new Configurable[filterLists.size()]);
    }

}
