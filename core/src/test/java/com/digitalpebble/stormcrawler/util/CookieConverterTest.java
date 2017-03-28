package com.digitalpebble.stormcrawler.util;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.cookie.Cookie;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.junit.Test;

import com.digitalpebble.stormcrawler.protocol.httpclient.HttpProtocol;

import org.junit.Assert;
import org.junit.Ignore;

public class CookieConverterTest {

	private static final PoolingHttpClientConnectionManager CONNECTION_MANAGER = new PoolingHttpClientConnectionManager();

	private static String securedUrl = "https://someurl.com";
	private static String unsecuredUrl = "http://someurl.com";
	private static String dummyCookieHeader = "nice tasty test cookie header!";
	private static String dummyCookieValue = "nice tasty test cookie value!";
	private String[] cookiesStrings;

	@Test
	public void testSimpleCookieAndUrl() {
		String[] cookiesStrings = new String[1];
		String dummyCookieString = buildCookieString(dummyCookieHeader, dummyCookieValue, null, null, null, null);
		cookiesStrings[0] = dummyCookieString;
		List<Cookie> result = CookieConverter.getCookies(cookiesStrings, getUrl(unsecuredUrl));
		Assert.assertEquals("Should have 1 cookie", 1, result.size());
		Assert.assertEquals("Cookie header should be as defined", dummyCookieHeader, result.get(0).getName());
		Assert.assertEquals("Cookie value should be as defined", dummyCookieValue, result.get(0).getValue());

	}

	@Test
	public void testNotExpiredCookie() {
		String[] cookiesStrings = new String[1];
		String dummyCookieString = buildCookieString(dummyCookieHeader, dummyCookieValue, null,
				"Tue, 11 Apr 2117 07:13:39 -0000", null, null);
		cookiesStrings[0] = dummyCookieString;
		List<Cookie> result = CookieConverter.getCookies(cookiesStrings, getUrl(unsecuredUrl));
		Assert.assertEquals("Should have 1 cookie", 1, result.size());
		Assert.assertEquals("Cookie header should be as defined", dummyCookieHeader, result.get(0).getName());
		Assert.assertEquals("Cookie value should be as defined", dummyCookieValue, result.get(0).getValue());

	}

	@Test
	public void testExpiredCookie() {
		String[] cookiesStrings = new String[1];
		String dummyCookieString = buildCookieString(dummyCookieHeader, dummyCookieValue, null,
				"Tue, 11 Apr 2016 07:13:39 -0000", null, null);
		cookiesStrings[0] = dummyCookieString;
		List<Cookie> result = CookieConverter.getCookies(cookiesStrings, getUrl(unsecuredUrl));
		Assert.assertEquals("Should have 0 cookies, since cookie was expired", 0, result.size());

	}

	@Test
	public void testValidPath() {
		String[] cookiesStrings = new String[1];
		String dummyCookieString = buildCookieString(dummyCookieHeader, dummyCookieValue, null, null, "/", null);
		cookiesStrings[0] = dummyCookieString;
		List<Cookie> result = CookieConverter.getCookies(cookiesStrings, getUrl(unsecuredUrl + "/somepage"));
		Assert.assertEquals("Should have 1 cookie", 1, result.size());
		Assert.assertEquals("Cookie header should be as defined", dummyCookieHeader, result.get(0).getName());
		Assert.assertEquals("Cookie value should be as defined", dummyCookieValue, result.get(0).getValue());

	}

	@Test
	public void testValidPath2() {
		String[] cookiesStrings = new String[1];
		String dummyCookieString = buildCookieString(dummyCookieHeader, dummyCookieValue, null, null, "/", null);
		cookiesStrings[0] = dummyCookieString;
		List<Cookie> result = CookieConverter.getCookies(cookiesStrings, getUrl(unsecuredUrl));
		Assert.assertEquals("Should have 1 cookie", 1, result.size());
		Assert.assertEquals("Cookie header should be as defined", dummyCookieHeader, result.get(0).getName());
		Assert.assertEquals("Cookie value should be as defined", dummyCookieValue, result.get(0).getValue());

	}

	@Test
	public void testValidPath3() {
		String[] cookiesStrings = new String[1];
		String dummyCookieString = buildCookieString(dummyCookieHeader, dummyCookieValue, null, null, "/someFolder",
				null);
		cookiesStrings[0] = dummyCookieString;
		List<Cookie> result = CookieConverter.getCookies(cookiesStrings, getUrl(unsecuredUrl + "/someFolder"));
		Assert.assertEquals("Should have 1 cookie", 1, result.size());
		Assert.assertEquals("Cookie header should be as defined", dummyCookieHeader, result.get(0).getName());
		Assert.assertEquals("Cookie value should be as defined", dummyCookieValue, result.get(0).getValue());

	}

	@Test
	public void testValidPath4() {
		String[] cookiesStrings = new String[1];
		String dummyCookieString = buildCookieString(dummyCookieHeader, dummyCookieValue, null, null, "/someFolder",
				null);
		cookiesStrings[0] = dummyCookieString;
		List<Cookie> result = CookieConverter.getCookies(cookiesStrings,
				getUrl(unsecuredUrl + "/someFolder/SomeOtherFolder"));
		Assert.assertEquals("Should have 1 cookie", 1, result.size());
		Assert.assertEquals("Cookie header should be as defined", dummyCookieHeader, result.get(0).getName());
		Assert.assertEquals("Cookie value should be as defined", dummyCookieValue, result.get(0).getValue());

	}

	@Test
	public void testInvalidPath() {
		String[] cookiesStrings = new String[1];
		String dummyCookieString = buildCookieString(dummyCookieHeader, dummyCookieValue, null, null, "/someFolder",
				null);
		cookiesStrings[0] = dummyCookieString;
		List<Cookie> result = CookieConverter.getCookies(cookiesStrings,
				getUrl(unsecuredUrl + "/someOtherFolder/SomeFolder"));
		Assert.assertEquals("path mismatch, should have 0 cookies", 0, result.size());

	}

	@Test
	public void testValidDomain() {
		String[] cookiesStrings = new String[1];
		String dummyCookieString = buildCookieString(dummyCookieHeader, dummyCookieValue, "someurl.com", null, null,
				null);
		cookiesStrings[0] = dummyCookieString;
		List<Cookie> result = CookieConverter.getCookies(cookiesStrings,
				getUrl(unsecuredUrl + "/someFolder/SomeOtherFolder"));
		Assert.assertEquals("Should have 1 cookie", 1, result.size());
		Assert.assertEquals("Cookie header should be as defined", dummyCookieHeader, result.get(0).getName());
		Assert.assertEquals("Cookie value should be as defined", dummyCookieValue, result.get(0).getValue());

	}

	@Test
	public void testInvalidDomain() {
		String[] cookiesStrings = new String[1];
		String dummyCookieString = buildCookieString(dummyCookieHeader, dummyCookieValue, "someOtherUrl.com", null,
				null, null);
		cookiesStrings[0] = dummyCookieString;
		List<Cookie> result = CookieConverter.getCookies(cookiesStrings,
				getUrl(unsecuredUrl + "/someFolder/SomeOtherFolder"));
		Assert.assertEquals("Domain is not valid - Should have 0 cookies", 0, result.size());
	}

	@Test
	public void testSecurFlagHttp() {
		String[] cookiesStrings = new String[1];
		String dummyCookieString = buildCookieString(dummyCookieHeader, dummyCookieValue, null, null, null,
				Boolean.TRUE);
		cookiesStrings[0] = dummyCookieString;
		List<Cookie> result = CookieConverter.getCookies(cookiesStrings,
				getUrl(unsecuredUrl + "/someFolder/SomeOtherFolder"));
		Assert.assertEquals("Target url is not secured - Should have 0 cookies", 0, result.size());
	}

	@Test
	public void testSecurFlagHttpS() {
		String[] cookiesStrings = new String[1];
		String dummyCookieString = buildCookieString(dummyCookieHeader, dummyCookieValue, null, null, null,
				Boolean.TRUE);
		cookiesStrings[0] = dummyCookieString;
		List<Cookie> result = CookieConverter.getCookies(cookiesStrings,
				getUrl(securedUrl + "/someFolder/SomeOtherFolder"));
		Assert.assertEquals("Target url is  secured - Should have 1 cookie", 1, result.size());
		Assert.assertEquals("Cookie header should be as defined", dummyCookieHeader, result.get(0).getName());
		Assert.assertEquals("Cookie value should be as defined", dummyCookieValue, result.get(0).getValue());
	}

	@Test
	public void testFullCookie() {
		String[] cookiesStrings = new String[1];
		String dummyCookieString = buildCookieString(dummyCookieHeader, dummyCookieValue, "someurl.com",
				"Tue, 11 Apr 2117 07:13:39 -0000", "/", true);
		cookiesStrings[0] = dummyCookieString;
		List<Cookie> result = CookieConverter.getCookies(cookiesStrings,
				getUrl(securedUrl + "/someFolder/SomeOtherFolder"));
		Assert.assertEquals("Should have 1 cookie", 1, result.size());
		Assert.assertEquals("Cookie header should be as defined", dummyCookieHeader, result.get(0).getName());
		Assert.assertEquals("Cookie value should be as defined", dummyCookieValue, result.get(0).getValue());
	}

	@Test
	public void test2Cookies() {
		String[] cookiesStrings = new String[2];
		String dummyCookieString = buildCookieString(dummyCookieHeader, dummyCookieValue, "someurl.com",
				"Tue, 11 Apr 2117 07:13:39 -0000", "/", true);
		String dummyCookieString2 = buildCookieString(dummyCookieHeader + "2", dummyCookieValue + "2", "someurl.com",
				"Tue, 11 Apr 2117 07:13:39 -0000", "/", true);
		cookiesStrings[0] = dummyCookieString;
		cookiesStrings[1] = dummyCookieString2;
		List<Cookie> result = CookieConverter.getCookies(cookiesStrings,
				getUrl(securedUrl + "/someFolder/SomeOtherFolder"));
		Assert.assertEquals("Should have 2 cookies", 2, result.size());
		Assert.assertEquals("Cookie header should be as defined", dummyCookieHeader, result.get(0).getName());
		Assert.assertEquals("Cookie value should be as defined", dummyCookieValue, result.get(0).getValue());

		Assert.assertEquals("Cookie header should be as defined", dummyCookieHeader + "2", result.get(1).getName());
		Assert.assertEquals("Cookie value should be as defined", dummyCookieValue + "2", result.get(1).getValue());
	}

	@Ignore // Useful for debugging full flow
	@Test
	public void testWebResponse() {
		HttpGet httpget = new HttpGet("https://github.com/");
		HttpClientBuilder builder = HttpClients.custom().setConnectionManager(CONNECTION_MANAGER)
				.setConnectionManagerShared(true).disableRedirectHandling().disableAutomaticRetries();
		HttpClient client = builder.build();
		HttpProtocol httpProtocol = new HttpProtocol();
		try {
			client.execute(httpget, httpProtocol);
		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private URL getUrl(String urlString) {
		try {
			return new URL(urlString);
		} catch (MalformedURLException e) {
			return null;
		}
	}

	private String buildCookieString(String header, String value, String domain, String expires, String path,
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
