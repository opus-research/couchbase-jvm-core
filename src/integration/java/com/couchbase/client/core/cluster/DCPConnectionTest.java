/*
 * Copyright (c) 2016 Couchbase, Inc.
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

package com.couchbase.client.core.cluster;

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.endpoint.dcp.DCPConnection;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.dcp.OpenConnectionRequest;
import com.couchbase.client.core.message.dcp.OpenConnectionResponse;
import com.couchbase.client.core.message.dcp.DCPMessage;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.core.message.dcp.SnapshotMarkerMessage;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.core.message.kv.UpsertRequest;
import com.couchbase.client.core.message.kv.UpsertResponse;
import com.couchbase.client.core.util.ClusterDependentTest;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.functions.Action1;
import rx.observers.TestSubscriber;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DCPConnectionTest extends ClusterDependentTest {

    @Before
    public void checkIfDCPEnabled() throws Exception {
        Assume.assumeTrue(isDCPEnabled());
    }

    private DCPConnection createConnection(String connectionName) {
        OpenConnectionRequest request = new OpenConnectionRequest(connectionName, bucket(), password());
        OpenConnectionResponse response = cluster().<OpenConnectionResponse>send(request).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, response.status());
        return response.connection();
    }

    @Test
    public void shouldAddAndRemoveStreamsToConnection() {
        DCPConnection connection = createConnection("shouldAddAndRemoveStreamsToConnection");

        ResponseStatus status;

        status = connection.addStream((short) 42).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, status);

        status = connection.addStream((short) 42).toBlocking().single();
        assertEquals(ResponseStatus.EXISTS, status);

        status = connection.removeStream((short) 42).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, status);

        status = connection.removeStream((short) 42).toBlocking().single();
        assertEquals(ResponseStatus.NOT_EXISTS, status);
    }

    @Test
    public void shouldRollbackOnInvalidRange() {
        DCPConnection connection = createConnection("shouldRollbackOnInvalidRange");

        ResponseStatus status;

        status = connection.addStream((short) 1, 0, 42, 42, 0, 0).toBlocking().single();
        assertEquals(ResponseStatus.SUCCESS, status);
    }

    @Test
    public void shouldReturnCurrentState() {
        DCPConnection connection = createConnection("shouldReturnCurrentState");

        List<MutationToken> state;

        state = connection.getCurrentState().toList().toBlocking().single();
        assertEquals(numberOfPartitions(), state.size());
        for (MutationToken token : state) {
            assertTrue(token.vbucketUUID() > 0);
        }
    }

    private void upsertKey(String key, String value) {
        UpsertResponse resp = cluster()
                .<UpsertResponse>send(new UpsertRequest(key, Unpooled.copiedBuffer(value, CharsetUtil.UTF_8), 1, 0, bucket()))
                .toBlocking()
                .single();
        assertEquals(ResponseStatus.SUCCESS, resp.status());
        ReferenceCountUtil.releaseLater(resp.content());

    }

    @Test
    public void shouldTransmitTheData() {
        DCPConnection connection = createConnection("shouldTransmitTheData");

        connection.addStream(calculateVBucketForKey("foo")).toBlocking().single();
        TestSubscriber<DCPRequest> subscriber = new TestSubscriber<DCPRequest>();
        connection.subject().takeUntil(Observable.timer(5, TimeUnit.SECONDS)).subscribe(subscriber);

        upsertKey("foo", "value");

        subscriber.awaitTerminalEvent();
        List<DCPRequest> items = subscriber.getOnNextEvents();

        boolean seenMutation = false;
        boolean seenSnapshot = false;
        for (DCPRequest found : items) {
            if (found instanceof SnapshotMarkerMessage) {
                seenSnapshot = true;
            } else if (found instanceof MutationMessage) {
                seenMutation = true;
                assertEquals("foo", ((MutationMessage) found).key());
                ReferenceCountUtil.releaseLater(((MutationMessage) found).content());
            }
        }

        assertTrue(seenMutation);
        assertTrue(seenSnapshot);
    }

    @Test
    public void shouldUseFlowControl() {
        final String fooValue = "shouldUseFlowControl---foo-value";
        final String barValue = "shouldUseFlowControl---bar-value";
        final DCPConnection connection = createConnection("shouldUseFlowControl");

        List<MutationToken> state = connection.getCurrentState().toList().toBlocking().single();
        assertEquals(numberOfPartitions(), state.size());
        for (MutationToken token : state) {
            connection.addStream(
                    (short) token.vbucketID(),
                    token.vbucketUUID(),
                    token.sequenceNumber(),
                    0xffffffff,
                    token.sequenceNumber(),
                    0xffffffff
            ).toBlocking().single();
        }

        final List<DCPRequest> items = Collections.synchronizedList(new ArrayList<DCPRequest>());
        connection.subject().takeUntil(Observable.timer(10, TimeUnit.SECONDS)).subscribe(new Action1<DCPRequest>() {
            @Override
            public void call(DCPRequest request) {
                items.add(request);
                if (request instanceof DCPMessage) {
                    connection.consumed((DCPMessage)request);
                }
            }
        });

        for (int i = 0; i < 10; i++) {
            upsertKey("foo", fooValue);
            upsertKey("bar", barValue);
        }

        int fooMutations = 0;
        int barMutations = 0;
        for (DCPRequest found : items) {
            if (found instanceof MutationMessage) {
                MutationMessage mutation = (MutationMessage) found;
                System.out.println(mutation);
                String key = mutation.key();
                if (key.equals("foo")) {
                    assertEquals(mutation.content().toString(CharsetUtil.UTF_8), fooValue);
                    fooMutations++;
                } else if (key.equals("bar")) {
                    assertEquals(mutation.content().toString(CharsetUtil.UTF_8), barValue);
                    barMutations++;
                } else {
                    fail("unexpected mutation of key: " + key);
                }
                ReferenceCountUtil.releaseLater(((MutationMessage) found).content());
            }
        }

        assertEquals(10, fooMutations);
        assertEquals(10, barMutations);
    }

    private int numberOfPartitions() {
        GetClusterConfigResponse res = cluster().<GetClusterConfigResponse>send(new GetClusterConfigRequest()).toBlocking().single();
        CouchbaseBucketConfig config = (CouchbaseBucketConfig) res.config().bucketConfig(bucket());
        return config.numberOfPartitions();
    }

    private short calculateVBucketForKey(String key) {
        CRC32 crc32 = new CRC32();
        try {
            crc32.update(key.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        long rv = (crc32.getValue() >> 16) & 0x7fff;
        return (short) ((int) rv & numberOfPartitions() - 1);
    }

}