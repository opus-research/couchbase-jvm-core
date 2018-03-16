/**
 * Copyright (c) 2014 Couchbase, Inc.
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

package com.couchbase.client.core;

import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.endpoint.ResponseStatusConverter;
import com.couchbase.client.core.endpoint.kv.KeyValueStatus;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.GetRequest;
import com.couchbase.client.core.message.kv.GetResponse;
import com.couchbase.client.core.message.kv.InsertRequest;
import com.couchbase.client.core.message.kv.InsertResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import rx.subjects.Subject;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies functionality of the {@link ResponseHandler}.
 *
 * @author Michael Nitschinger
 * @since 1.0.3
 */
public class ResponseHandlerTest {

    private static final CoreEnvironment ENVIRONMENT = DefaultCoreEnvironment.create();

    @Test
    public void shouldSendProposedConfigToProvider() throws Exception {
        ClusterFacade clusterMock = mock(ClusterFacade.class);
        ConfigurationProvider providerMock = mock(ConfigurationProvider.class);
        ResponseHandler handler = new ResponseHandler(ENVIRONMENT, clusterMock, providerMock);
        ByteBuf config = Unpooled.copiedBuffer("{\"json\": true}", CharsetUtil.UTF_8);

        ResponseEvent retryEvent = new ResponseEvent();
        retryEvent.setMessage(new InsertResponse(ResponseStatus.RETRY, KeyValueStatus.ERR_TEMP_FAIL.code(),
                0, "bucket", config, null, mock(InsertRequest.class)));
        retryEvent.setObservable(mock(Subject.class));
        handler.onEvent(retryEvent, 1, true);

        verify(providerMock, times(1)).proposeBucketConfig("bucket", "{\"json\": true}");
        assertEquals(0, config.refCnt());
        assertNull(retryEvent.getMessage());
        assertNull(retryEvent.getObservable());
    }

    @Test
    public void shouldIgnoreInvalidConfig() throws Exception {
        ClusterFacade clusterMock = mock(ClusterFacade.class);
        ConfigurationProvider providerMock = mock(ConfigurationProvider.class);
        ResponseHandler handler = new ResponseHandler(ENVIRONMENT, clusterMock, providerMock);
        ByteBuf config = Unpooled.copiedBuffer("Not my Vbucket", CharsetUtil.UTF_8);

        ResponseEvent retryEvent = new ResponseEvent();
        retryEvent.setMessage(new InsertResponse(ResponseStatus.RETRY, KeyValueStatus.ERR_TEMP_FAIL.code(),
                0, "bucket", config, null, mock(InsertRequest.class)));
        retryEvent.setObservable(mock(Subject.class));
        handler.onEvent(retryEvent, 1, true);

        verify(providerMock, never()).proposeBucketConfig("bucket", "Not my Vbucket");
        assertEquals(0, config.refCnt());
        assertNull(retryEvent.getMessage());
        assertNull(retryEvent.getObservable());
    }

    @Test
    public void shouldDispatchFirstNMVImmediately() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        ClusterFacade clusterMock = mock(ClusterFacade.class);
        when(clusterMock.send(any(CouchbaseRequest.class))).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                latch.countDown();
                return null;
            }
        });

        ConfigurationProvider providerMock = mock(ConfigurationProvider.class);
        ClusterConfig clusterConfig = mock(ClusterConfig.class);
        BucketConfig bucketConfig = mock(BucketConfig.class);
        when(providerMock.config()).thenReturn(clusterConfig);
        when(clusterConfig.bucketConfig("bucket")).thenReturn(bucketConfig);
        when(bucketConfig.hasFastForwardMap()).thenReturn(true);

        ResponseHandler handler = new ResponseHandler(ENVIRONMENT, clusterMock, providerMock);

        GetRequest request = new GetRequest("key", "bucket");
        GetResponse response = new GetResponse(ResponseStatus.RETRY, (short) 0, 0L ,0, "bucket", Unpooled.EMPTY_BUFFER,
                request);

        ResponseEvent retryEvent = new ResponseEvent();
        retryEvent.setMessage(response);
        retryEvent.setObservable(request.observable());

        handler.onEvent(retryEvent, 1, true);

        long start = System.nanoTime();
        latch.await(5, TimeUnit.SECONDS);
        long end = System.nanoTime();

        // assert immediate dispatch
        assertTrue(TimeUnit.NANOSECONDS.toMillis(end - start) < 10);
    }

    @Test
    public void shouldDispatchFirstNMVBWithDelayIfNoFFMap() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        ClusterFacade clusterMock = mock(ClusterFacade.class);
        when(clusterMock.send(any(CouchbaseRequest.class))).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                latch.countDown();
                return null;
            }
        });

        ConfigurationProvider providerMock = mock(ConfigurationProvider.class);
        ClusterConfig clusterConfig = mock(ClusterConfig.class);
        BucketConfig bucketConfig = mock(BucketConfig.class);
        when(providerMock.config()).thenReturn(clusterConfig);
        when(clusterConfig.bucketConfig("bucket")).thenReturn(bucketConfig);
        when(bucketConfig.hasFastForwardMap()).thenReturn(false);

        ResponseHandler handler = new ResponseHandler(ENVIRONMENT, clusterMock, providerMock);

        GetRequest request = new GetRequest("key", "bucket");
        GetResponse response = new GetResponse(ResponseStatus.RETRY, (short) 0, 0L ,0, "bucket", Unpooled.EMPTY_BUFFER,
                request);

        ResponseEvent retryEvent = new ResponseEvent();
        retryEvent.setMessage(response);
        retryEvent.setObservable(request.observable());

        handler.onEvent(retryEvent, 1, true);

        long start = System.nanoTime();
        latch.await(5, TimeUnit.SECONDS);
        long end = System.nanoTime();

        // assert delayed dispatch
        assertTrue(TimeUnit.NANOSECONDS.toMillis(end - start) >= 100);
    }

    @Test
    public void shouldDispatchSecondNMVWithDelay() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        ClusterFacade clusterMock = mock(ClusterFacade.class);
        when(clusterMock.send(any(CouchbaseRequest.class))).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                latch.countDown();
                return null;
            }
        });

        ConfigurationProvider providerMock = mock(ConfigurationProvider.class);
        ClusterConfig clusterConfig = mock(ClusterConfig.class);
        BucketConfig bucketConfig = mock(BucketConfig.class);
        when(providerMock.config()).thenReturn(clusterConfig);
        when(clusterConfig.bucketConfig("bucket")).thenReturn(bucketConfig);
        when(bucketConfig.hasFastForwardMap()).thenReturn(true);

        ResponseHandler handler = new ResponseHandler(ENVIRONMENT, clusterMock, providerMock);

        GetRequest request = new GetRequest("key", "bucket");
        request.incrementRetryCount(); // pretend its at least once retried!
        GetResponse response = new GetResponse(ResponseStatus.RETRY, (short) 0, 0L ,0, "bucket", Unpooled.EMPTY_BUFFER,
            request);

        ResponseEvent retryEvent = new ResponseEvent();
        retryEvent.setMessage(response);
        retryEvent.setObservable(request.observable());

        handler.onEvent(retryEvent, 1, true);

        long start = System.nanoTime();
        latch.await(5, TimeUnit.SECONDS);
        long end = System.nanoTime();

        // assert delayed dispatch
        assertTrue(TimeUnit.NANOSECONDS.toMillis(end - start) >= 100);
    }

}
