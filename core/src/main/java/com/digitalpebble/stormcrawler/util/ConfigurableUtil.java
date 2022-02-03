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
     * Calls {@link ConfigurableUtil#configure(String, Class, Map, JsonNode)} with {@code
     * caller.getName()} for {@code configName}.
     *
     * @see ConfigurableUtil#configure(String, Class, Map, JsonNode) for more information.
     */
    public static <T extends Configurable> List<T> configure(
            Class<?> caller,
            Class<T> filterClass,
            Map<String, Object> stormConf,
            JsonNode filtersConf) {
        return configure(caller.getName(), filterClass, stormConf, filtersConf);
    }

    /**
     * Used by classes like URLFilters and ParseFilters to load the configuration of utilized
     * filters from the provided JSON config.
     *
     * <p>The functions searches for a childNode in {@code filtersConf} with the given {@code
     * configName}. If the childNode is found it initializes all elements in the list provided by
     * the {@code filtersConf} and initialized them as {@code filterClass}.
     *
     * <p>The following snippet shows the JSON-Schema for a config file, if the config file does not
     * meet the schema, the function fails.
     *
     * <pre>{@code
     * {
     *   "$id": "https://stormcrawler.net/schemas/configurable/config",
     *   "$schema": "https://json-schema.org/draft/2020-12/schema",
     *   "type": "object",
     *   "properties": {
     *     <configName>: {
     *       "type": "array",
     *       "contains": {
     *         "type": "object",
     *         "properties": {
     *           "name": {
     *             "type": "string",
     *             "default": "<unnamed>"
     *           },
     *           "class": {
     *             "type": "string"
     *           },
     *           "properties": {
     *             "type": "array",
     *             "default": null
     *           }
     *         },
     *         "required": [
     *           "class"
     *         ]
     *       }
     *     }
     *   }
     * }
     * }</pre>
     */
    public static <T extends Configurable> List<T> configure(
            String configName,
            Class<T> filterClass,
            Map<String, Object> stormConf,
            JsonNode filtersConf) {
        // initialises the filters
        List<T> filterLists = new ArrayList<>();

        // get the filters part
        filtersConf = filtersConf.get(configName);
        if (filtersConf == null) {
            LOG.info("No field {} in JSON config. Skipping...", configName);
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
                T filterInstance =
                        InitialisationUtil.initializeFromQualifiedName(className, filterClass);

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
