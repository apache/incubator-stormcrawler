package com.digitalpebble.stormcrawler.protocol.okhttp;

import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.protocol.AbstractHttpProtocol;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import com.digitalpebble.stormcrawler.util.ConfUtils;

import crawlercommons.robots.BaseRobotRules;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class HttpProtocol extends AbstractHttpProtocol {

    private OkHttpClient client;

    @Override
    public void configure(Config conf) {
        super.configure(conf);

        int timeout = ConfUtils.getInt(conf, "http.timeout", 10000);

        client = new OkHttpClient.Builder().followRedirects(false)
                .connectTimeout(timeout, TimeUnit.MILLISECONDS)
                .writeTimeout(timeout, TimeUnit.MILLISECONDS)
                .readTimeout(timeout, TimeUnit.MILLISECONDS).build();
    }

    @Override
    public ProtocolResponse getProtocolOutput(String url, Metadata metadata)
            throws Exception {
        Request request = new Request.Builder().url(url).build();
        Response response = client.newCall(request).execute();

        return new ProtocolResponse(response.body().bytes(), response.code(),
                metadata);
    }

    public static void main(String args[]) throws Exception {
        HttpProtocol protocol = new HttpProtocol();
        Config conf = new Config();

        String url = args[0];
        ConfUtils.loadConf(args[1], conf);
        protocol.configure(conf);

        if (!protocol.skipRobots) {
            BaseRobotRules rules = protocol.getRobotRules(url);
            System.out.println("is allowed : " + rules.isAllowed(url));
        }

        Metadata md = new Metadata();
        ProtocolResponse response = protocol.getProtocolOutput(url, md);
        System.out.println(url);
        System.out.println("### REQUEST MD ###");
        System.out.println(md);
        System.out.println("### RESPONSE MD ###");
        System.out.println(response.getMetadata());
        System.out.println(response.getStatusCode());
        System.out.println(response.getContent().length);
    }

}
