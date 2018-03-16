package com.couchbase.client.core.util;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.CouchbaseCore;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.OpenBucketResponse;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.cluster.SeedNodesResponse;
import com.couchbase.client.core.message.config.FlushRequest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import rx.Observable;
import rx.functions.Func1;

/**
 * Base test class for tests that need a working cluster reference.
 *
 * @author Michael Nitschinger
 */
public class ClusterDependentTest {

    private static final String seedNode = TestProperties.seedNode();
    private static final String bucket = TestProperties.bucket();
    private static final String password = TestProperties.password();

    private static ClusterFacade cluster;

    @BeforeClass
    public static void connect() {
        cluster = new CouchbaseCore();
        cluster.<SeedNodesResponse>send(new SeedNodesRequest(seedNode)).flatMap(
            new Func1<SeedNodesResponse, Observable<OpenBucketResponse>>() {
                @Override
                public Observable<OpenBucketResponse> call(SeedNodesResponse response) {
                    return cluster.send(new OpenBucketRequest(bucket, password));
                }
            }
        ).toBlocking().single();
    }

    @AfterClass
    public static void disconnect() throws InterruptedException {
        // FIXME DisconnectRequest doesn't work anymore :(
        //cluster.send(new DisconnectRequest()).toBlocking().first();
    }

    public static void flush() {
        cluster.send(new FlushRequest(bucket, password)).toBlocking().single();
    }

    public static String password() {
        return password;
    }

    public static ClusterFacade cluster() {
        return cluster;
    }

    public static String bucket() {
        return bucket;
    }
}
