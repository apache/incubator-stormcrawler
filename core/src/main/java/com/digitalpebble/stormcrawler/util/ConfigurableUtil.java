package com.digitalpebble.stormcrawler.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.slf4j.LoggerFactory;

public final class ConfigurableUtil {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ConfigurableUtil.class);

    private ConfigurableUtil() {}

    /**
     * Used by classes URLFilters and ParseFilters classes to load the configuration of filters from
     * JSON
     */
    @SuppressWarnings("rawtypes")
    public static <T extends Configurable> List<T> configure(
            Map stormConf, JsonNode filtersConf, Class<T> filterClass, String callingClass) {
        // initialises the filters
        List<T> filterLists = new ArrayList<>();

        // get the filters part
        filtersConf = filtersConf.get(callingClass);

        if (filtersConf == null) {
            LOG.info("No field {} in JSON config. Skipping", callingClass);
            return filterLists;
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
                T filterInstance = (T) filterImplClass.getDeclaredConstructor().newInstance();

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

        return filterLists;
    }
}
