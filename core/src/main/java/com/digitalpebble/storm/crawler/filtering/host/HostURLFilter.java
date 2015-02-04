package com.digitalpebble.storm.crawler.filtering.host;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.filtering.URLFilter;
import com.fasterxml.jackson.databind.JsonNode;

import crawlercommons.url.PaidLevelDomain;

/**
 * Filters URL based on the hostname.
 * 
 * This filter has 2 modes:
 * <ul>
 * <li>if <code>ignoreOutsideHost</code> is <code>true</code>, all URLs with a
 * host different from the host of the source URL are filtered out</li>
 * <li>if <code>ignoreOutsideDomain</code> is <code>true</code>, all URLs with a
 * domain different from the source's domain are filtered out</li>
 * </ul>
 */
public class HostURLFilter implements URLFilter {

    private boolean ignoreOutsideHost;
    private boolean ignoreOutsideDomain;

    private URL previousSourceUrl;
    private String previousSourceHost;
    private String previousSourceDomain;

    @Override
    public void configure(Map stormConf, JsonNode filterParams) {
        JsonNode filterByHostNode = filterParams.get("ignoreOutsideHost");
        if (filterByHostNode == null) {
            ignoreOutsideHost = false;
        } else {
            ignoreOutsideHost = filterByHostNode.asBoolean(false);
        }

        // ignoreOutsideDomain is not necessary if we require the host to be
        // always the same
        if (!ignoreOutsideHost) {
            JsonNode filterByDomainNode = filterParams
                    .get("ignoreOutsideDomain");
            if (filterByHostNode == null) {
                ignoreOutsideDomain = false;
            } else {
                ignoreOutsideDomain = filterByDomainNode.asBoolean(false);
            }
        } else {
            ignoreOutsideDomain = false;
        }
    }

    @Override
    public String filter(URL sourceUrl, Metadata sourceMetadata,
            String urlToFilter) {
        if (sourceUrl == null || (!ignoreOutsideHost && !ignoreOutsideDomain)) {
            return urlToFilter;
        }

        URL tURL;
        try {
            tURL = new URL(urlToFilter);
        } catch (MalformedURLException e1) {
            return null;
        }

        String fromHost;
        String fromDomain = null;
        // Using identity comparison because URL.equals performs poorly
        if (sourceUrl == previousSourceUrl) {
            fromHost = previousSourceHost;
            if (ignoreOutsideDomain) {
                fromDomain = previousSourceDomain;
            }
        } else {
            fromHost = sourceUrl.getHost();
            if (ignoreOutsideDomain) {
                fromDomain = PaidLevelDomain.getPLD(fromHost);
            }
            previousSourceHost = fromHost;
            previousSourceDomain = fromDomain;
            previousSourceUrl = sourceUrl;
        }

        // resolve the hosts
        String toHost = tURL.getHost();

        if (ignoreOutsideHost) {
            if (toHost == null || !toHost.equalsIgnoreCase(fromHost)) {
                return null;
            }
        }

        if (ignoreOutsideDomain) {
            String toDomain = PaidLevelDomain.getPLD(toHost);
            if (toDomain == null || !toDomain.equals(fromDomain)) {
                return null;
            }
        }

        return urlToFilter;
    }

}
