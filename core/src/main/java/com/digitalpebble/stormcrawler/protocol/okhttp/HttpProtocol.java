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

package com.digitalpebble.stormcrawler.protocol.okhttp;

import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.protocol.AbstractHttpProtocol;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import com.digitalpebble.stormcrawler.util.ConfUtils;

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
        HttpProtocol.main(new HttpProtocol(), args);
    }

}
