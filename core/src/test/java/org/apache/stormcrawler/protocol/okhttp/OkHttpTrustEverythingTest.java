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

package org.apache.stormcrawler.protocol.okhttp;

import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.common.ConsoleNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Base64;
import java.util.List;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;
import org.apache.storm.Config;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for https://github.com/apache/stormcrawler/issues/2079: the trust-all
 * configuration of the okhttp protocol must be an explicit choice, must not disable hostname
 * verification and must not disclose credentials to servers which are not authenticated.
 */
/*
 * The keystores under src/test/resources/ssl are self-signed PKCS12 keystores, generated with
 * (JDK keytool, passwords are the "changeit" default, valid for 9000 days):
 *
 * keytool -genkeypair -alias localhost -keyalg RSA -keysize 2048 -validity 9000 \
 *   -dname "CN=localhost" -ext "SAN=dns:localhost,ip:127.0.0.1" \
 *   -keystore localhost.p12 -storetype PKCS12 -storepass changeit -keypass changeit
 *
 * keytool -genkeypair -alias otherhost -keyalg RSA -keysize 2048 -validity 9000 \
 *   -dname "CN=otherhost.invalid" -ext "SAN=dns:otherhost.invalid" \
 *   -keystore otherhost.p12 -storetype PKCS12 -storepass changeit -keypass changeit
 */
class OkHttpTrustEverythingTest {

    private static final String KEYSTORE_PASSWORD = "changeit";

    /** Certificate issued for localhost: valid for the host the tests connect to. */
    private static final String LOCALHOST_KEYSTORE = "/ssl/localhost.p12";

    /** Certificate issued for another host name: trusted under trust-all, wrong name. */
    private static final String OTHERHOST_KEYSTORE = "/ssl/otherhost.p12";

    private WireMockServer server;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop();
            server = null;
        }
    }

    @Test
    void trustAllContextUsesTls() {
        assertEquals(
                "TLS",
                HttpProtocol.trustAllSslContext.getProtocol(),
                "the trust-all SSLContext should be a TLS context");
    }

    @Test
    void selfSignedCertificateRejectedByDefault() throws Exception {
        // http.trust.everything defaults to false: an unvalidatable certificate
        // must not be accepted
        startServer(LOCALHOST_KEYSTORE);
        final HttpProtocol protocol = protocol(config());
        assertThrows(
                SSLHandshakeException.class,
                () -> fetch(protocol, "/default"),
                "self-signed certificates must be rejected by default");
    }

    @Test
    void trustAllFetchesServerWithSelfSignedCertificate() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        startServer(LOCALHOST_KEYSTORE);
        final ProtocolResponse response = fetch(protocol(conf), "/trustall");
        assertEquals(200, response.getStatusCode(), "the self-signed certificate is accepted");
    }

    @Test
    void hostnameIsStillVerified() throws Exception {
        // the certificate is issued for another host name: trusting any
        // certificate must not imply accepting any name
        final Config conf = config();
        conf.put("http.trust.everything", true);
        startServer(OTHERHOST_KEYSTORE);
        assertThrows(
                SSLPeerUnverifiedException.class,
                () -> fetch(protocol(conf), "/hostname"),
                "the hostname verifier should not accept any name unconditionally");
    }

    @Test
    void hostnameVerificationCanBeDisabledSeparately() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        conf.put("http.verify.hostnames", false);
        startServer(OTHERHOST_KEYSTORE);
        final ProtocolResponse response = fetch(protocol(conf), "/nohostnamecheck");
        assertEquals(200, response.getStatusCode(), "the name mismatch is accepted as configured");
    }

    @Test
    void basicAuthIsWithheldFromUnauthenticatedServers() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        conf.put("http.basicauth.user", "user");
        conf.put("http.basicauth.password", "secret");
        startServer(LOCALHOST_KEYSTORE);
        fetch(protocol(conf), "/basicauth");
        server.verify(
                1, getRequestedFor(urlPathEqualTo("/basicauth")).withoutHeader("Authorization"));
    }

    @Test
    void basicAuthIsSentWhenExplicitlyAllowed() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        conf.put("http.credentials.allow.insecure", true);
        conf.put("http.basicauth.user", "user");
        conf.put("http.basicauth.password", "secret");
        startServer(LOCALHOST_KEYSTORE);
        fetch(protocol(conf), "/basicauth");
        final String expected =
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString("user:secret".getBytes(StandardCharsets.UTF_8));
        server.verify(
                1,
                getRequestedFor(urlPathEqualTo("/basicauth"))
                        .withHeader("Authorization", equalTo(expected)));
    }

    @Test
    void credentialCustomHeadersAreWithheldFromUnauthenticatedServers() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        conf.put("http.custom.headers", List.of("X-Api-Key=s3cret", "X-Trace=public"));
        startServer(LOCALHOST_KEYSTORE);
        fetch(protocol(conf), "/customheaders");
        server.verify(
                1, getRequestedFor(urlPathEqualTo("/customheaders")).withoutHeader("X-Api-Key"));
        server.verify(
                1,
                getRequestedFor(urlPathEqualTo("/customheaders"))
                        .withHeader("X-Trace", equalTo("public")));
    }

    @Test
    void credentialCustomHeadersAreSentWhenExplicitlyAllowed() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        conf.put("http.credentials.allow.insecure", true);
        conf.put("http.custom.headers", List.of("X-Api-Key=s3cret", "X-Trace=public"));
        startServer(LOCALHOST_KEYSTORE);
        fetch(protocol(conf), "/customheaders");
        server.verify(
                1,
                getRequestedFor(urlPathEqualTo("/customheaders"))
                        .withHeader("X-Api-Key", equalTo("s3cret")));
    }

    @Test
    void cookiesAreWithheldFromUnauthenticatedServers() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        conf.put("http.use.cookies", true);
        startServer(LOCALHOST_KEYSTORE);
        fetch(protocol(conf), "/cookies", metadata());
        server.verify(1, getRequestedFor(urlPathEqualTo("/cookies")).withoutHeader("Cookie"));
    }

    @Test
    void cookiesAreSentWhenExplicitlyAllowed() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        conf.put("http.credentials.allow.insecure", true);
        conf.put("http.use.cookies", true);
        startServer(LOCALHOST_KEYSTORE);
        fetch(protocol(conf), "/cookies", metadata());
        server.verify(
                1,
                getRequestedFor(urlPathEqualTo("/cookies")).withHeader("Cookie", equalTo("sid=x")));
    }

    @Test
    void basicAuthIsWithheldOverCleartextHttp() throws Exception {
        // a cleartext http:// request does not authenticate the server either
        final Config conf = config();
        conf.put("http.basicauth.user", "user");
        conf.put("http.basicauth.password", "secret");
        startServer(LOCALHOST_KEYSTORE);
        final ProtocolResponse response =
                fetchUrl(protocol(conf), httpUrl("/cleartext"), new Metadata());
        assertEquals(200, response.getStatusCode(), "the connection must succeed");
        server.verify(
                1, getRequestedFor(urlPathEqualTo("/cleartext")).withoutHeader("Authorization"));
    }

    @Test
    void basicAuthIsSentOverCleartextHttpWhenExplicitlyAllowed() throws Exception {
        final Config conf = config();
        conf.put("http.credentials.allow.insecure", true);
        conf.put("http.basicauth.user", "user");
        conf.put("http.basicauth.password", "secret");
        startServer(LOCALHOST_KEYSTORE);
        fetchUrl(protocol(conf), httpUrl("/cleartext"), new Metadata());
        final String expected = "Basic " + base64("user:secret");
        server.verify(
                1,
                getRequestedFor(urlPathEqualTo("/cleartext"))
                        .withHeader("Authorization", equalTo(expected)));
    }

    @Test
    void credentialHeaderNamesCanBeConfigured() throws Exception {
        // http.credentials.headers replaces the default list
        final Config conf = config();
        conf.put("http.trust.everything", true);
        conf.put("http.credentials.headers", List.of("X-Auth-Token"));
        conf.put("http.custom.headers", List.of("X-Auth-Token=token1", "X-Api-Key=key1"));
        startServer(LOCALHOST_KEYSTORE);
        fetch(protocol(conf), "/configuredheaders");
        server.verify(
                1,
                getRequestedFor(urlPathEqualTo("/configuredheaders"))
                        .withoutHeader("X-Auth-Token"));
        server.verify(
                1,
                getRequestedFor(urlPathEqualTo("/configuredheaders"))
                        .withHeader("X-Api-Key", equalTo("key1")));
    }

    /** Metadata as an outlink would inherit it, with a cookie scoped to the server. */
    private Metadata metadata() {
        final Metadata md = new Metadata();
        md.setValue("protocol.set-cookie", "sid=x; Path=/");
        md.setValue("protocol.set-cookie-origin", "https://localhost:" + server.httpsPort() + "/");
        return md;
    }

    private String base64(String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private String httpUrl(String path) {
        return "http://localhost:" + server.port() + path;
    }

    private Config config() {
        final Config conf = new Config();
        conf.put("http.agent.name", "test");
        conf.put("http.agent.version", "1.0");
        conf.put("http.agent.description", "test");
        conf.put("http.agent.url", "http://test.example.com");
        conf.put("http.agent.email", "test@example.com");
        conf.put("protocol.md.prefix", "protocol.");
        return conf;
    }

    private HttpProtocol protocol(Config conf) {
        final HttpProtocol protocol = new HttpProtocol();
        protocol.configure(conf);
        return protocol;
    }

    private ProtocolResponse fetch(HttpProtocol protocol, String path) throws Exception {
        return fetch(protocol, path, new Metadata());
    }

    private ProtocolResponse fetch(HttpProtocol protocol, String path, Metadata md)
            throws Exception {
        return fetchUrl(protocol, "https://localhost:" + server.httpsPort() + path, md);
    }

    private ProtocolResponse fetchUrl(HttpProtocol protocol, String url, Metadata md)
            throws Exception {
        return protocol.getProtocolOutput(url, md);
    }

    /**
     * Starts an HTTPS server on a random port presenting the certificate of the keystore. The
     * keystore is copied to a temporary file as WireMock reads it from the file system.
     */
    private void startServer(String keystoreResource) throws Exception {
        final Path keystoreFile = Files.createTempFile("wiremock-keystore", ".p12");
        keystoreFile.toFile().deleteOnExit();
        try (InputStream in =
                OkHttpTrustEverythingTest.class.getResourceAsStream(keystoreResource)) {
            Files.copy(in, keystoreFile, StandardCopyOption.REPLACE_EXISTING);
        }

        server =
                new WireMockServer(
                        WireMockConfiguration.options()
                                .dynamicPort()
                                .dynamicHttpsPort()
                                .keystorePath(keystoreFile.toAbsolutePath().toString())
                                .keystorePassword(KEYSTORE_PASSWORD)
                                .keyManagerPassword(KEYSTORE_PASSWORD)
                                .keystoreType("PKCS12")
                                .notifier(new ConsoleNotifier(false)));
        server.start();
        server.stubFor(any(anyUrl()).willReturn(ok("Success!")));
    }
}
