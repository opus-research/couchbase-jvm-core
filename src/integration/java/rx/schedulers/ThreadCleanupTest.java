/*
 * Copyright (c) 2015 Couchbase, Inc.
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
package rx.schedulers;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.CouchbaseCore;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.cluster.DisconnectRequest;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.OpenBucketResponse;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.cluster.SeedNodesResponse;
import com.couchbase.client.core.util.TestProperties;
import org.junit.Test;
import rx.Observable;
import rx.functions.Func1;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * A test case that validates shutdown behavior of both the SDK and some external components
 * like RxJava. Aim is to detect potential problems with threading cleanup in containers like Tomcat.
 *
 * This test case doesn't extend ClusterDependentTest so that the connection and shutdown of the cluster
 * can be triggered at the right time for the tests.
 *
 * Since it shutdowns RxJava, it must restart it, hence being in the rx.schedulers package.
 *
 * @author Simon Baslé
 * @since 1.2
 */
public class ThreadCleanupTest {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(ThreadCleanupTest.class);

    private static final String seedNode = TestProperties.seedNode();
    private static final String bucket = TestProperties.bucket();
    private static final String password = TestProperties.password();

    private CoreEnvironment env;

    private ClusterFacade cluster;

    public void connect() {
        env = DefaultCoreEnvironment
                .builder()
                .dcpEnabled(true)
                .mutationTokensEnabled(true)
                .build();
        cluster = new CouchbaseCore(env);
        cluster.<SeedNodesResponse>send(new SeedNodesRequest(seedNode)).flatMap(
                new Func1<SeedNodesResponse, Observable<OpenBucketResponse>>() {
                    @Override
                    public Observable<OpenBucketResponse> call(SeedNodesResponse response) {
                        return cluster.send(new OpenBucketRequest(bucket, password));
                    }
                }
        ).toBlocking().single();
    }

    public void disconnect() throws InterruptedException {
        cluster.send(new DisconnectRequest()).toBlocking().first();
    }

    @Test
    public void testSdkNettyRxJavaThreadsShutdownProperly() throws InterruptedException {
        ThreadMXBean mx = ManagementFactory.getThreadMXBean();
        LOGGER.info("Threads at start");
        Set<String> ignore = dump(threads(mx));

        connect();

        LOGGER.info("Threads before shutdown:");
        dump(threads(mx, ignore));

        LOGGER.info("");
        LOGGER.info("Shutting Down Couchbase Cluster");
        disconnect();
        LOGGER.info("Shutting Down Couchbase Env");
        env.shutdown();
        LOGGER.info("Shutting Down RxJava");
        Schedulers.shutdown();
        LOGGER.info("");
        LOGGER.info("Threads after shutdown:");
        Set<String> afterShutdown = dump(threads(mx, ignore));
        if (afterShutdown.isEmpty())
        LOGGER.info("No threads remaining after shutdown");

        //restart RxJava for subsequent tests
        LOGGER.info("Restarting RxJava");
        Schedulers.start();

        assertEquals(0, afterShutdown.size());
    }

    private Set<String> dump(Set<String> threads) {
        for (String thread : threads) {
            LOGGER.info(thread);
        }
        return threads;
    }

    private Set<String> threads(ThreadMXBean mx, Set<String> ignore) {
        Set<String> all = threads(mx);
        all.removeAll(ignore);
        return all;
    }

    private Set<String> threads(ThreadMXBean mx) {
        ThreadInfo[] dump = mx.getThreadInfo(mx.getAllThreadIds());
        Set<String> names = new HashSet<String>(dump.length);
        for (ThreadInfo threadInfo : dump) {
            if (threadInfo == null || threadInfo.getThreadName() == null) {
                continue;
            }

            names.add(threadInfo.getThreadName());
        }
        return names;
    }
}
