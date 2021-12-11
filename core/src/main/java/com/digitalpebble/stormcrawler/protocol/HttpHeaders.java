/**
 * Licensed to DigitalPebble Ltd under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.digitalpebble.stormcrawler.protocol;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;

/**
 * A collection of HTTP header names and utilities around header values.
 *
 * @see <a href="http://rfc-ref.org/RFC-TEXTS/2616/">Hypertext Transfer Protocol -- HTTP/1.1 (RFC
 *     2616)</a>
 */
public final class HttpHeaders {

    private HttpHeaders() {}

    public static final String TRANSFER_ENCODING = "transfer-encoding";

    public static final String CONTENT_ENCODING = "content-encoding";

    public static final String CONTENT_LANGUAGE = "content-language";

    public static final String CONTENT_LENGTH = "content-length";

    public static final String CONTENT_LOCATION = "content-location";

    public static final String CONTENT_DISPOSITION = "content-disposition";

    public static final String CONTENT_MD5 = "content-md5";

    public static final String CONTENT_TYPE = "content-type";

    public static final String LAST_MODIFIED = "last-modified";

    public static final String LOCATION = "location";

    /**
     * Formatter for dates in HTTP headers, used to fill the &quot;If-Modified-Since&quot; request
     * header field, e.g.
     *
     * <pre>
     * Sun, 06 Nov 1994 08:49:37 GMT
     * </pre>
     *
     * See <a href= "https://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.3.1">sec. 3.3.1 in
     * RFC 2616</a> and <a href="https://tools.ietf.org/html/rfc7231#section-7.1.1.1">sec. 7.1.1.1
     * in RFC 7231</a>. The latter specifies the format defined in RFC 1123 as the
     * &quot;preferred&quot; format.
     */
    public static final DateTimeFormatter HTTP_DATE_FORMATTER =
            DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.ROOT)
                    .withZone(ZoneId.of(ZoneOffset.UTC.toString()));

    /** Formatter to parse ISO-formatted dates persisted in status index */
    public static final DateTimeFormatter ISO_INSTANT_FORMATTER =
            DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.of(ZoneOffset.UTC.toString()));

    /**
     * Format an ISO date string as HTTP date used in HTTP headers, e.g.,
     *
     * <pre>
     * 1994-11-06T08:49:37.000Z
     * </pre>
     *
     * is formatted to
     *
     * <pre>
     * Sun, 06 Nov 1994 08:49:37 GMT
     * </pre>
     *
     * See {@link #HTTP_DATE_FORMATTER}
     */
    public static String formatHttpDate(String isoDate) {
        try {
            ZonedDateTime date = ISO_INSTANT_FORMATTER.parse(isoDate, ZonedDateTime::from);
            return HTTP_DATE_FORMATTER.format(date);
        } catch (DateTimeParseException e) {
            // not an ISO date
            return "";
        }
    }
}
