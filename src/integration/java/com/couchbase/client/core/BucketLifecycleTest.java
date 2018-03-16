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
package com.couchbase.client.core;

import com.couchbase.client.core.config.BucketType;
import com.couchbase.client.core.config.ConfigurationException;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.cluster.CloseBucketRequest;
import com.couchbase.client.core.message.cluster.DisconnectRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.OpenBucketResponse;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.config.InsertBucketRequest;
import com.couchbase.client.core.message.config.RemoveBucketRequest;
import com.couchbase.client.core.message.internal.AddNodeRequest;
import com.couchbase.client.core.message.internal.AddNodeResponse;
import com.couchbase.client.core.message.internal.AddServiceRequest;
import com.couchbase.client.core.message.internal.AddServiceResponse;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.TestProperties;
import org.junit.Test;
import rx.Observable;
import rx.functions.Func0;
import rx.functions.Func1;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Verifies basic bucket lifecycles like opening and closing of valid and invalid buckets.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class BucketLifecycleTest {

    @Test
    public void shouldSuccessfullyOpenBucket() {
        CouchbaseCore core = new CouchbaseCore();

        core.send(new SeedNodesRequest(Arrays.asList(TestProperties.seedNode())));
        OpenBucketRequest request = new OpenBucketRequest(TestProperties.bucket(), TestProperties.password());
        Observable<OpenBucketResponse> response = core.send(request);
        assertEquals(ResponseStatus.SUCCESS, response.toBlocking().single().status());
    }

    @Test(expected = ConfigurationException.class)
    public void shouldFailWithNoSeedNodeList() {
        OpenBucketRequest request = new OpenBucketRequest(TestProperties.bucket(), TestProperties.password());
        new CouchbaseCore().send(request).toBlocking().single();
    }

    @Test(expected = ConfigurationException.class)
    public void shouldFailWithEmptySeedNodeList() {
        CouchbaseCore core = new CouchbaseCore();
        core.send(new SeedNodesRequest(Collections.<String>emptyList()));
        OpenBucketRequest request = new OpenBucketRequest(TestProperties.bucket(), TestProperties.password());
        core.send(request).toBlocking().single();
    }

    @Test(expected = ConfigurationException.class)
    public void shouldFailOpeningNonExistentBucket() {
        CouchbaseCore core = new CouchbaseCore();

        core.send(new SeedNodesRequest(Arrays.asList(TestProperties.seedNode())));
        OpenBucketRequest request = new OpenBucketRequest(TestProperties.bucket() + "asd", TestProperties.password());
        core.send(request).toBlocking().single();
    }

    @Test(expected = ConfigurationException.class)
    public void shouldFailOpeningBucketWithWrongPassword() {
        CouchbaseCore core = new CouchbaseCore();

        core.send(new SeedNodesRequest(Arrays.asList(TestProperties.seedNode())));
        OpenBucketRequest request = new OpenBucketRequest(TestProperties.bucket(), TestProperties.password() + "asd");
        core.send(request).toBlocking().single();
    }

    @Test(expected = ConfigurationException.class)
    public void shouldFailOpeningWithWrongHost() {
        CouchbaseCore core = new CouchbaseCore();

        core.send(new SeedNodesRequest(Arrays.asList("certainlyInvalidHostname")));
        OpenBucketRequest request = new OpenBucketRequest(TestProperties.bucket(), TestProperties.password() + "asd");
        core.send(request).toBlocking().single();
    }

    @Test
    public void shouldSucceedSubsequentlyAfterFailedAttempt() {
        final CouchbaseCore core = new CouchbaseCore();

        core.send(new SeedNodesRequest(Arrays.asList(TestProperties.seedNode())));

        OpenBucketRequest badAttempt = new OpenBucketRequest(TestProperties.bucket() + "asd", TestProperties.password());
        final OpenBucketRequest goodAttempt = new OpenBucketRequest(TestProperties.bucket(), TestProperties.password());

        OpenBucketResponse response = core
            .<OpenBucketResponse>send(badAttempt)
            .onErrorResumeNext(Observable.defer(new Func0<Observable<OpenBucketResponse>>() {
                @Override
                public Observable<OpenBucketResponse> call() {
                    return core.send(goodAttempt);
                }
            }))
            .timeout(10, TimeUnit.SECONDS)
            .toBlocking()
            .single();

        assertEquals(ResponseStatus.SUCCESS, response.status());
    }

    @Test
    public void shouldRemoveBucketWithoutCausingAuthErrors() throws InterruptedException, UnknownHostException {
        final CouchbaseCore core = new CouchbaseCore();

        String testBucket = "testInsertRemoveBucket" + TestProperties.bucket();
        final StringBuilder sb = new StringBuilder();
        sb.append("name=").append(testBucket);
        sb.append("&ramQuotaMB=100&authType=sasl&saslPassword=").append(TestProperties.password());
        sb.append("&replicaNumber=0&proxyPort=0&bucketType=membase&flushEnabled=0");

        SeedNodesRequest seedNodes = new SeedNodesRequest(Arrays.asList(TestProperties.seedNode()));
        OpenBucketRequest openDefault = new OpenBucketRequest(TestProperties.bucket(), TestProperties.password());
        InsertBucketRequest insertAdhoc = new InsertBucketRequest(sb.toString(), TestProperties.adminUser(), TestProperties.adminPassword());
        OpenBucketRequest openAdhoc = new OpenBucketRequest(testBucket, TestProperties.password());
        RemoveBucketRequest removeAdhoc = new RemoveBucketRequest(testBucket, TestProperties.adminUser(), TestProperties.adminPassword());
        CloseBucketRequest closeAdhoc = new CloseBucketRequest(testBucket);
        CloseBucketRequest closeDefault = new CloseBucketRequest(TestProperties.bucket());

        assertEquals("Error seeding nodes", ResponseStatus.SUCCESS, core.send(seedNodes).toBlocking().single().status());
        assertEquals("Error opening default bucket", ResponseStatus.SUCCESS, core.send(openDefault).toBlocking().single().status());
        assertEquals("Error inserting adhoc", ResponseStatus.SUCCESS, core.send(insertAdhoc).toBlocking().single().status());
        Thread.sleep(2000);
        assertEquals("Error opening adhoc", ResponseStatus.SUCCESS, core.send(openAdhoc).toBlocking().single().status());
        assertEquals("Error closing adhoc", ResponseStatus.SUCCESS, core.send(closeAdhoc).toBlocking().single().status());

        //reproduces the work done by clusterManager before delete bucket
        //only useful if you close both buckets before the remove, as then node list will be empty
        final InetAddress seedAddress = InetAddress.getByName(TestProperties.seedNode());
        core.<AddNodeResponse>send(new AddNodeRequest(seedAddress))
                        .flatMap(new Func1<AddNodeResponse, Observable<AddServiceResponse>>() {
                            @Override
                            public Observable<AddServiceResponse> call(AddNodeResponse response) {
                                return core.send(new AddServiceRequest(ServiceType.CONFIG, TestProperties.adminUser(),
                                        TestProperties.adminPassword(),8091, seedAddress));
                            }
                        })
                .toBlocking().single();
        assertEquals("Error removing adhoc", ResponseStatus.SUCCESS, core.send(removeAdhoc).toBlocking().single().status());

        //moving this just below the closing of adhoc doesn't trigger the auth issue
        assertEquals("Error closing default", ResponseStatus.SUCCESS, core.send(closeDefault).toBlocking().single().status());

        core.send(new DisconnectRequest());
    }

}
