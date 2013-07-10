package com.digitalpebble.storm.crawler.fetcher.asynchttpclient;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import com.digitalpebble.storm.crawler.fetcher.Protocol;
import com.digitalpebble.storm.crawler.fetcher.ProtocolResponse;
import com.digitalpebble.storm.crawler.util.Configuration;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.AsyncHttpClientConfig.Builder;
import com.ning.http.client.ProxyServer;
import com.ning.http.client.Response;

/*** Wrapper for Asynchttpclient lib as protocol handler **/
public class AHProtocol implements Protocol {

    private AsyncHttpClient client;

    @Override
    public void configure(Configuration conf) {
        String agentString = getAgentString(conf.get("http.agent.name"),
                conf.get("http.agent.version"),
                conf.get("http.agent.description"), conf.get("http.agent.url"),
                conf.get("http.agent.email"));

        Builder builder = new AsyncHttpClientConfig.Builder();
        builder.setUserAgent(agentString);

        // proxy?
        String proxyHost = conf.get("http.proxy.host");
        int proxyPort = conf.getInt("http.proxy.port", 8080);

        if (proxyHost != null) {
            ProxyServer proxyServer = new ProxyServer(proxyHost, proxyPort);
            builder.setProxyServer(proxyServer);
        }

        builder.setCompressionEnabled(true);

        client = new AsyncHttpClient(builder.build());

    }

    private static String getAgentString(String agentName, String agentVersion,
            String agentDesc, String agentURL, String agentEmail) {

        if ((agentName == null) || (agentName.trim().length() == 0)) {
            agentName = "Anonymous coward";
        }

        StringBuffer buf = new StringBuffer();

        buf.append(agentName);
        if (agentVersion != null) {
            buf.append("/");
            buf.append(agentVersion.trim());
        }
        if (((agentDesc != null) && (agentDesc.length() != 0))
                || ((agentEmail != null) && (agentEmail.length() != 0))
                || ((agentURL != null) && (agentURL.length() != 0))) {
            buf.append(" (");

            if ((agentDesc != null) && (agentDesc.length() != 0)) {
                buf.append(agentDesc.trim());
                if ((agentURL != null) || (agentEmail != null))
                    buf.append("; ");
            }

            if ((agentURL != null) && (agentURL.length() != 0)) {
                buf.append(agentURL.trim());
                if (agentEmail != null)
                    buf.append("; ");
            }

            if ((agentEmail != null) && (agentEmail.length() != 0))
                buf.append(agentEmail.trim());

            buf.append(")");
        }
        return buf.toString();
    }

    @Override
    public ProtocolResponse getProtocolOutput(String url) throws Exception {
        Response response = client.prepareGet(url).execute().get();
        final byte[] content = response.getResponseBodyAsBytes();
        final int statusCode = response.getStatusCode();
        HashMap<String, String[]> metadata = new HashMap<String, String[]>();

        Iterator<Entry<String, List<String>>> iter = response.getHeaders()
                .iterator();

        while (iter.hasNext()) {
            Entry<String, List<String>> entry = iter.next();
            String[] sval = entry.getValue().toArray(
                    new String[entry.getValue().size()]);

            metadata.put("fetch." + entry.getKey(), sval);
        }

        return new ProtocolResponse(content, statusCode, metadata);
    }

}
