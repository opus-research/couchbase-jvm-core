/*
 * Copyright (c) 2017 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.cluster;

import com.couchbase.client.core.endpoint.ResponseStatusConverter;
import com.couchbase.client.core.endpoint.kv.ErrorMap;
import com.couchbase.client.core.message.kv.UpsertRequest;
import com.couchbase.client.core.message.kv.UpsertResponse;
import com.couchbase.client.core.util.ClusterDependentTest;
import com.couchbase.mock.JsonUtils;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * Basic error map test
 *
 * @author Subhashni Balakrishnan
 * @since 1.4.5
 */
public class KeyValueErrorMapTest extends ClusterDependentTest {

    @BeforeClass
    public static void setup() throws Exception {
        connect(true);
    }

    private static void startRetryVerifyRequest() throws Exception {
        URIBuilder builder = new URIBuilder();
        builder.setScheme("http").setHost("localhost").setPort(mock().getHttpPort()).setPath("mock/start_retry_verify")
                .setParameter("idx", "0")
                .setParameter("bucket", bucket());
        HttpGet request = new HttpGet(builder.build());
        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = client.execute(request);
        int status = response.getStatusLine().getStatusCode();
        if (status != 200) {
            throw new ClientProtocolException("Unexpected response status: " + status);
        }
        String rawBody = EntityUtils.toString(response.getEntity());
        com.google.gson.JsonObject respObject = JsonUtils.GSON.fromJson(rawBody, com.google.gson.JsonObject.class);
        String verifyStatus = respObject.get("status").getAsString();
        if (verifyStatus.compareTo("ok") != 0) {
            throw new Exception(respObject.get("error").getAsString());
        }
    }

    private static void checkRetryVerifyRequest(long code) throws Exception {
        URIBuilder builder = new URIBuilder();
        builder.setScheme("http").setHost("localhost").setPort(mock().getHttpPort()).setPath("mock/check_retry_verify")
                .setParameter("idx", "0")
                .setParameter("opcode", "1")
                .setParameter("errcode", Long.toString(code))
                .setParameter("bucket", bucket());
        HttpGet request = new HttpGet(builder.build());
        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = client.execute(request);
        int status = response.getStatusLine().getStatusCode();
        if (status != 200) {
            throw new ClientProtocolException("Unexpected response status: " + status);
        }
        String rawBody = EntityUtils.toString(response.getEntity());
        com.google.gson.JsonObject respObject = JsonUtils.GSON.fromJson(rawBody, com.google.gson.JsonObject.class);
        String verifyStatus = respObject.get("status").getAsString();
        if (verifyStatus.compareTo("ok") != 0) {
            throw new Exception(respObject.get("error").getAsString());
        }
    }

    private static void opFailRequest(long code, int count) throws Exception {
        URIBuilder builder = new URIBuilder();
        builder.setScheme("http").setHost("localhost").setPort(mock().getHttpPort()).setPath("mock/opfail")
                .setParameter("code", Long.toString(code))
                .setParameter("count", Integer.toString(count))
                .setParameter("servers", "[0]");
        HttpGet request = new HttpGet(builder.build());
        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = client.execute(request);
        int status = response.getStatusLine().getStatusCode();
        if (status != 200) {
            throw new ClientProtocolException("Unexpected response status: " + status);
        }
        String rawBody = EntityUtils.toString(response.getEntity());
        com.google.gson.JsonObject respObject = JsonUtils.GSON.fromJson(rawBody, com.google.gson.JsonObject.class);
        String verifyStatus = respObject.get("status").getAsString();
        if (verifyStatus.compareTo("ok") != 0) {
            throw new Exception(respObject.get("error").getAsString());
        }
    }

    @Test
    public void checkIfTheErrorMapIsRead() throws Exception {
        ErrorMap errMap = ResponseStatusConverter.getBinaryErrorMap();
        assertNotNull(errMap);
    }

    @Test
    public void verifyConstantRetry() throws Exception {
        opFailRequest(Long.parseLong("7FF0", 16), 20);
        startRetryVerifyRequest();
        String key = "upsert-key";
        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer("", CharsetUtil.UTF_8), bucket());
        UpsertResponse response = cluster().<UpsertResponse>send(upsert).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());
        checkRetryVerifyRequest(Long.parseLong("7FF0", 16));
    }

    @Test
    public void verifyLinearRetry() throws Exception {
        opFailRequest(Long.parseLong("7FF1", 16), 10);
        startRetryVerifyRequest();
        String key = "upsert-key";
        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer("", CharsetUtil.UTF_8), bucket());
        UpsertResponse response = cluster().<UpsertResponse>send(upsert).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());
        checkRetryVerifyRequest(Long.parseLong("7FF1", 16));
    }

    @Test
    public void verifyExponentialRetry() throws Exception {
        opFailRequest(Long.parseLong("7FF2", 16), 5);
        startRetryVerifyRequest();
        String key = "upsert-key";
        UpsertRequest upsert = new UpsertRequest(key, Unpooled.copiedBuffer("", CharsetUtil.UTF_8), bucket());
        UpsertResponse response = cluster().<UpsertResponse>send(upsert).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());
        checkRetryVerifyRequest(Long.parseLong("7FF2", 16));
    }
}
