/**
 * Copyright (C) 2014 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.client.core.config.loader;

import com.couchbase.client.core.cluster.Cluster;
import com.couchbase.client.core.env.CouchbaseEnvironment;
import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.binary.GetBucketConfigRequest;
import com.couchbase.client.core.message.binary.GetBucketConfigResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import org.junit.Test;
import rx.Observable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the correct functionality of the {@link CarrierLoader}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class CarrierLoaderTest {

    @Test
    public void shouldUseDirectPortIfNotSSL() {
        Environment environment = new CouchbaseEnvironment();
        Cluster cluster = mock(Cluster.class);

        CarrierLoader loader = new CarrierLoader(cluster, environment);
        assertEquals(environment.bootstrapCarrierDirectPort(), loader.port(environment));
    }

    @Test
    public void shouldUseEncryptedPortIfSSL() {
        Environment environment = mock(Environment.class);
        when(environment.sslEnabled()).thenReturn(true);
        when(environment.bootstrapCarrierSslPort()).thenReturn(12345);
        Cluster cluster = mock(Cluster.class);

        CarrierLoader loader = new CarrierLoader(cluster, environment);
        assertEquals(environment.bootstrapCarrierSslPort(), loader.port(environment));
    }

    @Test
    public void shouldDiscoverConfig() {
        Environment environment = new CouchbaseEnvironment();
        Cluster cluster = mock(Cluster.class);
        ByteBuf content = Unpooled.copiedBuffer("myconfig", CharsetUtil.UTF_8);
        Observable<CouchbaseResponse> response = Observable.from(
                (CouchbaseResponse) new GetBucketConfigResponse(ResponseStatus.SUCCESS, "bucket", content, "localhost")
        );
        when(cluster.send(isA(GetBucketConfigRequest.class))).thenReturn(response);

        CarrierLoader loader = new CarrierLoader(cluster, environment);
        Observable<String> configObservable = loader.discoverConfig("bucket", "password", "localhost");
        assertEquals("myconfig", configObservable.toBlockingObservable().single());
    }

    @Test
    public void shouldThrowExceptionIfNotDiscovered() {
        Environment environment = new CouchbaseEnvironment();
        Cluster cluster = mock(Cluster.class);
        Observable<CouchbaseResponse> response = Observable.from(
                (CouchbaseResponse) new GetBucketConfigResponse(ResponseStatus.FAILURE, "bucket", null, "localhost")
        );
        when(cluster.send(isA(GetBucketConfigRequest.class))).thenReturn(response);

        CarrierLoader loader = new CarrierLoader(cluster, environment);
        Observable<String> configObservable = loader.discoverConfig("bucket", "password", "localhost");
        try {
            configObservable.toBlockingObservable().single();
            assertTrue(false);
        } catch(IllegalStateException ex) {
            assertEquals("Bucket config response did not return with success.", ex.getMessage());
        } catch(Exception ex) {
            assertTrue(false);
        }
    }
}