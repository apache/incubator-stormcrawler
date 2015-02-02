/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.digitalpebble.storm.crawler.protocol;

/**
 * A collection of HTTP header names.
 * 
 * @see <a href="http://rfc-ref.org/RFC-TEXTS/2616/">Hypertext Transfer Protocol
 *      -- HTTP/1.1 (RFC 2616)</a>
 */
public interface HttpHeaders {

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

}
