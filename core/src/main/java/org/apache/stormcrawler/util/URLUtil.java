/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.stormcrawler.util;

import java.net.IDN;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Locale;
import java.util.regex.Pattern;

/** Utility class for URL analysis */
public class URLUtil {

    private URLUtil() {}

    /**
     * Resolve relative URL-s and fix a few java.net.URL errors in handling of URLs with embedded
     * params and pure query targets.
     *
     * @param base base url
     * @param target target url (may be relative)
     * @return resolved absolute url.
     * @throws MalformedURLException
     */
    public static URL resolveURL(URL base, String target) throws MalformedURLException {
        target = target.trim();

        if (target.startsWith("?")) {
            return fixPureQueryTargets(base, target);
        }

        return new URL(base, target);
    }

    /** Handle the case in RFC3986 section 5.4.1 example 7, and similar. */
    static URL fixPureQueryTargets(final URL base, String target) throws MalformedURLException {
        final String basePath = base.getPath();
        int baseRightMostIdx = basePath.lastIndexOf("/");
        if (baseRightMostIdx != -1) {
            final String baseRightMost = basePath.substring(baseRightMostIdx + 1);
            target = baseRightMost + target;
        }
        return new URL(base, target);
    }

    /**
     * Handles cases where the url param information is encoded into the base url as opposed to the
     * target.
     *
     * <p>If the target contains params (i.e. ';xxxx') information then the target params
     * information is assumed to be correct and any base params information is ignored. If the base
     * contains params information but the tareget does not, then the params information is moved to
     * the target allowing it to be correctly determined by the java.net.URL class.
     *
     * @param base The base URL.
     * @param target The target path from the base URL.
     * @return URL A URL with the params information correctly encoded.
     * @throws MalformedURLException If the url is not a well formed URL.
     */
    private static URL fixEmbeddedParams(URL base, String target) throws MalformedURLException {

        // the target contains params information or the base doesn't then no
        // conversion necessary, return regular URL
        if (target.indexOf(';') >= 0 || base.toString().indexOf(';') == -1) {
            return new URL(base, target);
        }

        // get the base url and it params information
        String baseURL = base.toString();
        int startParams = baseURL.indexOf(';');
        String params = baseURL.substring(startParams);

        // if the target has a query string then put the params information
        // after
        // any path but before the query string, otherwise just append to the
        // path
        int startQS = target.indexOf('?');
        if (startQS >= 0) {
            target = target.substring(0, startQS) + params + target.substring(startQS);
        } else {
            target += params;
        }

        return new URL(base, target);
    }

    private static Pattern IP_PATTERN = Pattern.compile("(\\d{1,3}\\.){3}(\\d{1,3})");

    /** Partitions of the hostname of the url by "." */
    public static String[] getHostSegments(URL url) {
        String host = url.getHost();
        // return whole hostname, if it is an ipv4
        // TODO : handle ipv6
        if (IP_PATTERN.matcher(host).matches()) return new String[] {host};
        return host.split("\\.");
    }

    /**
     * Partitions of the hostname of the url by "."
     *
     * @throws MalformedURLException
     */
    public static String[] getHostSegments(String url) throws MalformedURLException {
        return getHostSegments(new URL(url));
    }

    /**
     * Returns the lowercased hostname for the url or null if the url is not well formed.
     *
     * @param url The url to check.
     * @return String The hostname for the url.
     */
    public static String getHost(String url) {
        try {
            return new URL(url).getHost().toLowerCase(Locale.ROOT);
        } catch (MalformedURLException e) {
            return null;
        }
    }

    /**
     * Returns the page for the url. The page consists of the protocol, host, and path, but does not
     * include the query string. The host is lowercased but the path is not.
     *
     * @param url The url to check.
     * @return String The page for the url.
     */
    public static String getPage(String url) {
        try {
            // get the full url, and replace the query string with and empty
            // string
            url = url.toLowerCase(Locale.ROOT);
            String queryStr = new URL(url).getQuery();
            return (queryStr != null) ? url.replace("?" + queryStr, "") : url;
        } catch (MalformedURLException e) {
            return null;
        }
    }

    public static String toASCII(String url) {
        try {
            URL u = new URL(url);
            URI p =
                    new URI(
                            u.getProtocol(),
                            null,
                            IDN.toASCII(u.getHost()),
                            u.getPort(),
                            u.getPath(),
                            u.getQuery(),
                            u.getRef());

            return p.toString();
        } catch (Exception e) {
            return null;
        }
    }

    public static String toUNICODE(String url) {
        try {
            URL u = new URL(url);
            URI p =
                    new URI(
                            u.getProtocol(),
                            null,
                            IDN.toUnicode(u.getHost()),
                            u.getPort(),
                            u.getPath(),
                            u.getQuery(),
                            u.getRef());

            return p.toString();
        } catch (Exception e) {
            return null;
        }
    }
}
