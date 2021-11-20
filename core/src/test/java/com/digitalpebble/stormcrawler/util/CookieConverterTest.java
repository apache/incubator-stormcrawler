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
package com.digitalpebble.stormcrawler.util;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import org.apache.http.cookie.Cookie;
import org.junit.Assert;
import org.junit.Test;

public class CookieConverterTest {

    private static String securedUrl = "https://someurl.com";
    private static String unsecuredUrl = "http://someurl.com";
    private static String dummyCookieHeader = "nice tasty test cookie header!";
    private static String dummyCookieValue = "nice tasty test cookie value!";

    @Test
    public void testSimpleCookieAndUrl() {
        String[] cookiesStrings = new String[1];
        String dummyCookieString =
                buildCookieString(dummyCookieHeader, dummyCookieValue, null, null, null, null);
        cookiesStrings[0] = dummyCookieString;
        List<Cookie> result = CookieConverter.getCookies(cookiesStrings, getUrl(unsecuredUrl));
        Assert.assertEquals("Should have 1 cookie", 1, result.size());
        Assert.assertEquals(
                "Cookie header should be as defined", dummyCookieHeader, result.get(0).getName());
        Assert.assertEquals(
                "Cookie value should be as defined", dummyCookieValue, result.get(0).getValue());
    }

    @Test
    public void testNotExpiredCookie() {
        String[] cookiesStrings = new String[1];
        String dummyCookieString =
                buildCookieString(
                        dummyCookieHeader,
                        dummyCookieValue,
                        null,
                        "Tue, 11 Apr 2117 07:13:39 -0000",
                        null,
                        null);
        cookiesStrings[0] = dummyCookieString;
        List<Cookie> result = CookieConverter.getCookies(cookiesStrings, getUrl(unsecuredUrl));
        Assert.assertEquals("Should have 1 cookie", 1, result.size());
        Assert.assertEquals(
                "Cookie header should be as defined", dummyCookieHeader, result.get(0).getName());
        Assert.assertEquals(
                "Cookie value should be as defined", dummyCookieValue, result.get(0).getValue());
    }

    @Test
    public void testExpiredCookie() {
        String[] cookiesStrings = new String[1];
        String dummyCookieString =
                buildCookieString(
                        dummyCookieHeader,
                        dummyCookieValue,
                        null,
                        "Tue, 11 Apr 2016 07:13:39 -0000",
                        null,
                        null);
        cookiesStrings[0] = dummyCookieString;
        List<Cookie> result = CookieConverter.getCookies(cookiesStrings, getUrl(unsecuredUrl));
        Assert.assertEquals("Should have 0 cookies, since cookie was expired", 0, result.size());
    }

    @Test
    public void testValidPath() {
        String[] cookiesStrings = new String[1];
        String dummyCookieString =
                buildCookieString(dummyCookieHeader, dummyCookieValue, null, null, "/", null);
        cookiesStrings[0] = dummyCookieString;
        List<Cookie> result =
                CookieConverter.getCookies(cookiesStrings, getUrl(unsecuredUrl + "/somepage"));
        Assert.assertEquals("Should have 1 cookie", 1, result.size());
        Assert.assertEquals(
                "Cookie header should be as defined", dummyCookieHeader, result.get(0).getName());
        Assert.assertEquals(
                "Cookie value should be as defined", dummyCookieValue, result.get(0).getValue());
    }

    @Test
    public void testValidPath2() {
        String[] cookiesStrings = new String[1];
        String dummyCookieString =
                buildCookieString(dummyCookieHeader, dummyCookieValue, null, null, "/", null);
        cookiesStrings[0] = dummyCookieString;
        List<Cookie> result = CookieConverter.getCookies(cookiesStrings, getUrl(unsecuredUrl));
        Assert.assertEquals("Should have 1 cookie", 1, result.size());
        Assert.assertEquals(
                "Cookie header should be as defined", dummyCookieHeader, result.get(0).getName());
        Assert.assertEquals(
                "Cookie value should be as defined", dummyCookieValue, result.get(0).getValue());
    }

    @Test
    public void testValidPath3() {
        String[] cookiesStrings = new String[1];
        String dummyCookieString =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, null, null, "/someFolder", null);
        cookiesStrings[0] = dummyCookieString;
        List<Cookie> result =
                CookieConverter.getCookies(cookiesStrings, getUrl(unsecuredUrl + "/someFolder"));
        Assert.assertEquals("Should have 1 cookie", 1, result.size());
        Assert.assertEquals(
                "Cookie header should be as defined", dummyCookieHeader, result.get(0).getName());
        Assert.assertEquals(
                "Cookie value should be as defined", dummyCookieValue, result.get(0).getValue());
    }

    @Test
    public void testValidPath4() {
        String[] cookiesStrings = new String[1];
        String dummyCookieString =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, null, null, "/someFolder", null);
        cookiesStrings[0] = dummyCookieString;
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(unsecuredUrl + "/someFolder/SomeOtherFolder"));
        Assert.assertEquals("Should have 1 cookie", 1, result.size());
        Assert.assertEquals(
                "Cookie header should be as defined", dummyCookieHeader, result.get(0).getName());
        Assert.assertEquals(
                "Cookie value should be as defined", dummyCookieValue, result.get(0).getValue());
    }

    @Test
    public void testInvalidPath() {
        String[] cookiesStrings = new String[1];
        String dummyCookieString =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, null, null, "/someFolder", null);
        cookiesStrings[0] = dummyCookieString;
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(unsecuredUrl + "/someOtherFolder/SomeFolder"));
        Assert.assertEquals("path mismatch, should have 0 cookies", 0, result.size());
    }

    @Test
    public void testValidDomain() {
        String[] cookiesStrings = new String[1];
        String dummyCookieString =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, "someurl.com", null, null, null);
        cookiesStrings[0] = dummyCookieString;
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(unsecuredUrl + "/someFolder/SomeOtherFolder"));
        Assert.assertEquals("Should have 1 cookie", 1, result.size());
        Assert.assertEquals(
                "Cookie header should be as defined", dummyCookieHeader, result.get(0).getName());
        Assert.assertEquals(
                "Cookie value should be as defined", dummyCookieValue, result.get(0).getValue());
    }

    @Test
    public void testInvalidDomain() {
        String[] cookiesStrings = new String[1];
        String dummyCookieString =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, "someOtherUrl.com", null, null, null);
        cookiesStrings[0] = dummyCookieString;
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(unsecuredUrl + "/someFolder/SomeOtherFolder"));
        Assert.assertEquals("Domain is not valid - Should have 0 cookies", 0, result.size());
    }

    @Test
    public void testSecurFlagHttp() {
        String[] cookiesStrings = new String[1];
        String dummyCookieString =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, null, null, null, Boolean.TRUE);
        cookiesStrings[0] = dummyCookieString;
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(unsecuredUrl + "/someFolder/SomeOtherFolder"));
        Assert.assertEquals("Target url is not secured - Should have 0 cookies", 0, result.size());
    }

    @Test
    public void testSecurFlagHttpS() {
        String[] cookiesStrings = new String[1];
        String dummyCookieString =
                buildCookieString(
                        dummyCookieHeader, dummyCookieValue, null, null, null, Boolean.TRUE);
        cookiesStrings[0] = dummyCookieString;
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(securedUrl + "/someFolder/SomeOtherFolder"));
        Assert.assertEquals("Target url is  secured - Should have 1 cookie", 1, result.size());
        Assert.assertEquals(
                "Cookie header should be as defined", dummyCookieHeader, result.get(0).getName());
        Assert.assertEquals(
                "Cookie value should be as defined", dummyCookieValue, result.get(0).getValue());
    }

    @Test
    public void testFullCookie() {
        String[] cookiesStrings = new String[1];
        String dummyCookieString =
                buildCookieString(
                        dummyCookieHeader,
                        dummyCookieValue,
                        "someurl.com",
                        "Tue, 11 Apr 2117 07:13:39 -0000",
                        "/",
                        true);
        cookiesStrings[0] = dummyCookieString;
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(securedUrl + "/someFolder/SomeOtherFolder"));
        Assert.assertEquals("Should have 1 cookie", 1, result.size());
        Assert.assertEquals(
                "Cookie header should be as defined", dummyCookieHeader, result.get(0).getName());
        Assert.assertEquals(
                "Cookie value should be as defined", dummyCookieValue, result.get(0).getValue());
    }

    @Test
    public void test2Cookies() {
        String[] cookiesStrings = new String[2];
        String dummyCookieString =
                buildCookieString(
                        dummyCookieHeader,
                        dummyCookieValue,
                        "someurl.com",
                        "Tue, 11 Apr 2117 07:13:39 -0000",
                        "/",
                        true);
        String dummyCookieString2 =
                buildCookieString(
                        dummyCookieHeader + "2",
                        dummyCookieValue + "2",
                        "someurl.com",
                        "Tue, 11 Apr 2117 07:13:39 -0000",
                        "/",
                        true);
        cookiesStrings[0] = dummyCookieString;
        cookiesStrings[1] = dummyCookieString2;
        List<Cookie> result =
                CookieConverter.getCookies(
                        cookiesStrings, getUrl(securedUrl + "/someFolder/SomeOtherFolder"));
        Assert.assertEquals("Should have 2 cookies", 2, result.size());
        Assert.assertEquals(
                "Cookie header should be as defined", dummyCookieHeader, result.get(0).getName());
        Assert.assertEquals(
                "Cookie value should be as defined", dummyCookieValue, result.get(0).getValue());

        Assert.assertEquals(
                "Cookie header should be as defined",
                dummyCookieHeader + "2",
                result.get(1).getName());
        Assert.assertEquals(
                "Cookie value should be as defined",
                dummyCookieValue + "2",
                result.get(1).getValue());
    }

    @Test
    public void testDomainsChecker() {
        boolean result = CookieConverter.checkDomainMatchToUrl(".example.com", "www.example.com");
        Assert.assertEquals("domain is valid", true, result);
    }

    @Test
    public void testDomainsChecker2() {
        boolean result = CookieConverter.checkDomainMatchToUrl(".example.com", "example.com");
        Assert.assertEquals("domain is valid", true, result);
    }

    @Test
    public void testDomainsChecker3() {
        boolean result = CookieConverter.checkDomainMatchToUrl("example.com", "www.example.com");
        Assert.assertEquals("domain is valid", true, result);
    }

    @Test
    public void testDomainsChecker4() {
        boolean result = CookieConverter.checkDomainMatchToUrl("example.com", "anotherexample.com");
        Assert.assertEquals("domain is not valid", false, result);
    }

    private URL getUrl(String urlString) {
        try {
            return new URL(urlString);
        } catch (MalformedURLException e) {
            return null;
        }
    }

    private String buildCookieString(
            String header,
            String value,
            String domain,
            String expires,
            String path,
            Boolean secure) {
        StringBuilder builder = new StringBuilder(buildCookiePart(header, value));
        if (domain != null) {
            builder.append(buildCookiePart("domain", domain));
        }

        if (expires != null) {
            builder.append(buildCookiePart("expires", expires));
        }

        if (path != null) {
            builder.append(buildCookiePart("path", path));
        }

        if (secure != null) {
            builder.append("secure;");
        }

        return builder.toString();
    }

    private String buildCookiePart(String partName, String partValue) {
        return partName + "=" + partValue + ";";
    }
}
