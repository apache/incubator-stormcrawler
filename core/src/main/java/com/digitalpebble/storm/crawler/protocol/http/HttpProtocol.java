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

package com.digitalpebble.storm.crawler.protocol.http;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.protocol.Protocol;
import com.digitalpebble.storm.crawler.protocol.ProtocolResponse;
import com.digitalpebble.storm.crawler.util.ConfUtils;

import crawlercommons.robots.BaseRobotRules;

public class HttpProtocol implements Protocol {

    public static final int BUFFER_SIZE = 8 * 1024;

    /** The default logger */
    protected static final Logger LOGGER = LoggerFactory
            .getLogger(HttpProtocol.class);

    /** The proxy hostname. */
    protected String proxyHost = null;

    /** The proxy port. */
    protected int proxyPort = 8080;

    /** Indicates if a proxy is used */
    protected boolean useProxy = false;

    /** The network timeout in millisecond */
    protected int timeout = 10000;

    /** The length limit for downloaded content, in bytes. */
    protected int maxContent = 64 * 1024;

    /** The Nutch 'User-Agent' request header */
    protected String userAgent = getAgentString("NutchCVS", null, "Nutch",
            "http://nutch.apache.org/bot.html", "agent@nutch.apache.org");

    /** The "Accept-Language" request header value. */
    protected String acceptLanguage = "en-us,en-gb,en;q=0.7,*;q=0.3";

    /** The "Accept" request header value. */
    protected String accept = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";

    /** The specified logger */
    protected Logger logger = LOGGER;

    private Config conf = null;

    /** Do we use HTTP/1.1? */
    protected boolean useHttp11 = false;

    /**
     * Record response time in CrawlDatum's meta data, see property
     * http.store.responsetime.
     */
    protected boolean responseTime = true;

    /** Skip page if Crawl-Delay longer than this value. */
    protected long maxCrawlDelay = -1L;

    /** Which TLS/SSL protocols to support */
    protected Set<String> tlsPreferredProtocols;

    /** Which TLS/SSL cipher suites to support */
    protected Set<String> tlsPreferredCipherSuites;

    private com.digitalpebble.storm.crawler.protocol.http.HttpRobotRulesParser robots;

    /** Creates a new instance of HttpProtocol */
    public HttpProtocol() {
        this(null);
    }

    /** Creates a new instance of HttpProtocol */
    public HttpProtocol(Logger logger) {
        if (logger != null) {
            this.logger = logger;
        }
        // robots = new HttpRobotRulesParser();
    }

    @Override
    public void configure(Config conf) {
        this.conf = conf;
        this.proxyHost = ConfUtils.getString(conf, "http.proxy.host", null);
        this.proxyPort = ConfUtils.getInt(conf, "http.proxy.port", 8080);
        this.useProxy = (proxyHost != null && proxyHost.length() > 0);
        this.timeout = ConfUtils.getInt(conf, "http.timeout", 10000);
        this.maxContent = ConfUtils.getInt(conf, "http.content.limit",
                64 * 1024);
        this.userAgent = getAgentString(
                ConfUtils.getString(conf, "http.agent.name"),
                ConfUtils.getString(conf, "http.agent.version"),
                ConfUtils.getString(conf, "http.agent.description"),
                ConfUtils.getString(conf, "http.agent.url"),
                ConfUtils.getString(conf, "http.agent.email"));
        this.acceptLanguage = ConfUtils.getString(conf, "http.accept.language",
                acceptLanguage);
        this.accept = ConfUtils.getString(conf, "http.accept", accept);
        // backward-compatible default setting
        this.useHttp11 = ConfUtils.getBoolean(conf, "http.useHttp11", false);
        this.responseTime = ConfUtils.getBoolean(conf,
                "http.store.responsetime", true);
        // this.robots.setConf(conf);

        String[] protocols = new String[] { "TLSv1.2", "TLSv1.1", "TLSv1",
                "SSLv3" };
        String[] ciphers = new String[] {
                "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",
                "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
                "TLS_RSA_WITH_AES_256_CBC_SHA256",
                "TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA384",
                "TLS_ECDH_RSA_WITH_AES_256_CBC_SHA384",
                "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256",
                "TLS_DHE_DSS_WITH_AES_256_CBC_SHA256",
                "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
                "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
                "TLS_RSA_WITH_AES_256_CBC_SHA",
                "TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA",
                "TLS_ECDH_RSA_WITH_AES_256_CBC_SHA",
                "TLS_DHE_RSA_WITH_AES_256_CBC_SHA",
                "TLS_DHE_DSS_WITH_AES_256_CBC_SHA",
                "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
                "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
                "TLS_RSA_WITH_AES_128_CBC_SHA256",
                "TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA256",
                "TLS_ECDH_RSA_WITH_AES_128_CBC_SHA256",
                "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256",
                "TLS_DHE_DSS_WITH_AES_128_CBC_SHA256",
                "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",
                "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
                "TLS_RSA_WITH_AES_128_CBC_SHA",
                "TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA",
                "TLS_ECDH_RSA_WITH_AES_128_CBC_SHA",
                "TLS_DHE_RSA_WITH_AES_128_CBC_SHA",
                "TLS_DHE_DSS_WITH_AES_128_CBC_SHA",
                "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA",
                "TLS_ECDHE_RSA_WITH_RC4_128_SHA", "SSL_RSA_WITH_RC4_128_SHA",
                "TLS_ECDH_ECDSA_WITH_RC4_128_SHA",
                "TLS_ECDH_RSA_WITH_RC4_128_SHA",
                "TLS_ECDHE_ECDSA_WITH_3DES_EDE_CBC_SHA",
                "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA",
                "SSL_RSA_WITH_3DES_EDE_CBC_SHA",
                "TLS_ECDH_ECDSA_WITH_3DES_EDE_CBC_SHA",
                "TLS_ECDH_RSA_WITH_3DES_EDE_CBC_SHA",
                "SSL_DHE_RSA_WITH_3DES_EDE_CBC_SHA",
                "SSL_DHE_DSS_WITH_3DES_EDE_CBC_SHA",
                "SSL_RSA_WITH_RC4_128_MD5",
                "TLS_EMPTY_RENEGOTIATION_INFO_SCSV",
                "TLS_RSA_WITH_NULL_SHA256", "TLS_ECDHE_ECDSA_WITH_NULL_SHA",
                "TLS_ECDHE_RSA_WITH_NULL_SHA", "SSL_RSA_WITH_NULL_SHA",
                "TLS_ECDH_ECDSA_WITH_NULL_SHA", "TLS_ECDH_RSA_WITH_NULL_SHA",
                "SSL_RSA_WITH_NULL_MD5", "SSL_RSA_WITH_DES_CBC_SHA",
                "SSL_DHE_RSA_WITH_DES_CBC_SHA", "SSL_DHE_DSS_WITH_DES_CBC_SHA",
                "TLS_KRB5_WITH_RC4_128_SHA", "TLS_KRB5_WITH_RC4_128_MD5",
                "TLS_KRB5_WITH_3DES_EDE_CBC_SHA",
                "TLS_KRB5_WITH_3DES_EDE_CBC_MD5", "TLS_KRB5_WITH_DES_CBC_SHA",
                "TLS_KRB5_WITH_DES_CBC_MD5" };

        tlsPreferredProtocols = new HashSet<String>(Arrays.asList(protocols));
        tlsPreferredCipherSuites = new HashSet<String>(Arrays.asList(ciphers));

        robots = new HttpRobotRulesParser(conf);

        logConf();
    }

    // Inherited Javadoc
    public Config getConf() {
        return this.conf;
    }

    @Override
    public ProtocolResponse getProtocolOutput(String urlString,
            Metadata knownMetadata) throws Exception {

        URL u = new URL(urlString);

        long startTime = System.currentTimeMillis();
        HttpResponse response = new HttpResponse(this, u, knownMetadata); // make a request
        Metadata metadata = response.getHeaders();

        if (this.responseTime) {
            int elapsedTime = (int) (System.currentTimeMillis() - startTime);
            metadata.setValue("_rst_", Integer.toString(elapsedTime));
        }

        int code = response.getCode();
        byte[] content = response.getContent();

        return new ProtocolResponse(content, code, metadata);
    }

    public String getProxyHost() {
        return proxyHost;
    }

    public int getProxyPort() {
        return proxyPort;
    }

    public boolean useProxy() {
        return useProxy;
    }

    public int getTimeout() {
        return timeout;
    }

    public int getMaxContent() {
        return maxContent;
    }

    public String getUserAgent() {
        return userAgent;
    }

    /**
     * Value of "Accept-Language" request header sent by Nutch.
     *
     * @return The value of the header "Accept-Language" header.
     */
    public String getAcceptLanguage() {
        return acceptLanguage;
    }

    public String getAccept() {
        return accept;
    }

    public boolean getUseHttp11() {
        return useHttp11;
    }

    public Set<String> getTlsPreferredCipherSuites() {
        return tlsPreferredCipherSuites;
    }

    public Set<String> getTlsPreferredProtocols() {
        return tlsPreferredProtocols;
    }

    private static String getAgentString(String agentName, String agentVersion,
            String agentDesc, String agentURL, String agentEmail) {

        if ((agentName == null) || (agentName.trim().length() == 0)) {
            // TODO : NUTCH-258
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("No User-Agent string set (http.agent.name)!");
            }
        }

        StringBuffer buf = new StringBuffer();

        buf.append(agentName);
        if (agentVersion != null) {
            buf.append("/");
            buf.append(agentVersion);
        }
        if (((agentDesc != null) && (agentDesc.length() != 0))
                || ((agentEmail != null) && (agentEmail.length() != 0))
                || ((agentURL != null) && (agentURL.length() != 0))) {
            buf.append(" (");

            if ((agentDesc != null) && (agentDesc.length() != 0)) {
                buf.append(agentDesc);
                if ((agentURL != null) || (agentEmail != null))
                    buf.append("; ");
            }

            if ((agentURL != null) && (agentURL.length() != 0)) {
                buf.append(agentURL);
                if (agentEmail != null)
                    buf.append("; ");
            }

            if ((agentEmail != null) && (agentEmail.length() != 0))
                buf.append(agentEmail);

            buf.append(")");
        }
        return buf.toString();
    }

    protected void logConf() {
        if (logger.isInfoEnabled()) {
            logger.info("http.proxy.host = " + proxyHost);
            logger.info("http.proxy.port = " + proxyPort);
            logger.info("http.timeout = " + timeout);
            logger.info("http.content.limit = " + maxContent);
            logger.info("http.agent = " + userAgent);
            logger.info("http.accept.language = " + acceptLanguage);
            logger.info("http.accept = " + accept);
        }
    }

    public byte[] processGzipEncoded(byte[] compressed, URL url)
            throws IOException {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("uncompressing....");
        }

        // content can be empty (i.e. redirection) in which case
        // there is nothing to unzip
        if (compressed.length == 0)
            return compressed;

        byte[] content;
        if (getMaxContent() >= 0) {
            content = GZIPUtils.unzipBestEffort(compressed, getMaxContent());
        } else {
            content = GZIPUtils.unzipBestEffort(compressed);
        }

        if (content == null)
            throw new IOException("unzipBestEffort returned null");

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("fetched " + compressed.length
                    + " bytes of compressed content (expanded to "
                    + content.length + " bytes) from " + url);
        }
        return content;
    }

    public byte[] processDeflateEncoded(byte[] compressed, URL url)
            throws IOException {

        // content can be empty (i.e. redirection) in which case
        // there is nothing to deflate
        if (compressed.length == 0)
            return compressed;

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("inflating....");
        }

        byte[] content = DeflateUtils.inflateBestEffort(compressed,
                getMaxContent());

        if (content == null)
            throw new IOException("inflateBestEffort returned null");

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("fetched " + compressed.length
                    + " bytes of compressed content (expanded to "
                    + content.length + " bytes) from " + url);
        }
        return content;
    }

    @Override
    public BaseRobotRules getRobotRules(String url) {
        return robots.getRobotRulesSet(this, url);
    }
}
