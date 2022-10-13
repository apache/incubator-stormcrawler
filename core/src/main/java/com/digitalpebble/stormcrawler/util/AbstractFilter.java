package com.digitalpebble.stormcrawler.util;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import org.jetbrains.annotations.NotNull;

/** */
public abstract class AbstractFilter implements Configurable {

    private String name;

    @Override
    public void configure(
            @NotNull Map<String, Object> stormConf,
            @NotNull String filterName,
            @NotNull JsonNode filtersConf) {
        this.name = filterName;
        configure(stormConf, filtersConf);
    }

    /**
     * Get filter name.
     *
     * @return a {@link String}.
     */
    public String getName() {
        return this.name;
    }
}
