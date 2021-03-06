/*
 * Copyright (c) 2016 Couchbase, Inc.
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
package com.couchbase.client.core.config.refresher;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.DefaultNodeInfo;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.endpoint.kv.KeyValueStatus;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.CoreScheduler;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.GetBucketConfigRequest;
import com.couchbase.client.core.message.kv.GetBucketConfigResponse;
import com.couchbase.client.core.utils.NetworkAddress;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import rx.Observable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link CarrierRefresher}.
 *
 * @author Michael Nitschinger
 * @since 1.0.3
 */
public class CarrierRefresherTest {

    private static final CoreEnvironment ENVIRONMENT = DefaultCoreEnvironment.create();

    @Test
    public void shouldProposeConfigFromTaintedPoller() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);
        ConfigurationProvider provider = mock(ConfigurationProvider.class);
        BucketConfig config = mock(BucketConfig.class);

        CarrierRefresher refresher = new CarrierRefresher(ENVIRONMENT, cluster);
        refresher.provider(provider);

        when(config.name()).thenReturn("bucket");
        List<NodeInfo> nodeInfos = new ArrayList<NodeInfo>();
        Map<String, Integer> ports = new HashMap<String, Integer>();
        ports.put("direct", 11210);
        nodeInfos.add(new DefaultNodeInfo(null, "localhost:8091", ports));
        when(config.nodes()).thenReturn(nodeInfos);

        final AtomicReference<ByteBuf> bufRef = new AtomicReference<ByteBuf>(null);
        when(cluster.send(any(GetBucketConfigRequest.class)))
                .thenAnswer(new Answer<Observable<GetBucketConfigResponse>>() {
                    @Override
                    public Observable<GetBucketConfigResponse> answer(InvocationOnMock invocation) throws Throwable {
                        ByteBuf content = Unpooled.copiedBuffer("{\"config\": true}", CharsetUtil.UTF_8);
                        ByteBuf oldContent = bufRef.getAndSet(content);
                        if (oldContent != null) {
                            assertEquals(0, oldContent.refCnt());
                        }
                        GetBucketConfigResponse response = new GetBucketConfigResponse(
                                ResponseStatus.SUCCESS, KeyValueStatus.SUCCESS.code(),
                                "bucket",
                                content,
                                NetworkAddress.localhost());
                        return Observable.just(response);
                    }
                });

        refresher.markTainted(config);

        Thread.sleep(1500);

        verify(provider, times(1)).proposeBucketConfig("bucket", "{\"config\": true}");
        assertEquals(0, bufRef.get().refCnt());
    }

    @Test
    public void shouldNotProposeInvalidConfigFromTaintedPoller() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);
        ConfigurationProvider provider = mock(ConfigurationProvider.class);
        BucketConfig config = mock(BucketConfig.class);

        CarrierRefresher refresher = new CarrierRefresher(ENVIRONMENT, cluster);
        refresher.provider(provider);

        when(config.name()).thenReturn("bucket");
        List<NodeInfo> nodeInfos = new ArrayList<NodeInfo>();
        Map<String, Integer> ports = new HashMap<String, Integer>();
        ports.put("direct", 11210);
        nodeInfos.add(new DefaultNodeInfo(null, "localhost:8091", ports));
        when(config.nodes()).thenReturn(nodeInfos);

        ByteBuf content = Unpooled.copiedBuffer("", CharsetUtil.UTF_8);
        when(cluster.send(any(GetBucketConfigRequest.class))).thenReturn(Observable.just(
            (CouchbaseResponse) new GetBucketConfigResponse(
                ResponseStatus.FAILURE, KeyValueStatus.ERR_NOT_FOUND.code(),
                "bucket",
                content,
                NetworkAddress.localhost()
            )
        ));

        refresher.markTainted(config);

        Thread.sleep(1500);

        verify(provider, never()).proposeBucketConfig("bucket", "");
        assertEquals(0, content.refCnt());
    }

    @Test
    public void shouldRefreshWithValidClusterConfig() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);
        CarrierRefresher refresher = new CarrierRefresher(ENVIRONMENT, cluster);
        refresher.registerBucket("bucket", "");
        ConfigurationProvider provider = mock(ConfigurationProvider.class);
        refresher.provider(provider);

        ClusterConfig clusterConfig = mock(ClusterConfig.class);
        BucketConfig bucketConfig = mock(BucketConfig.class);
        when(bucketConfig.name()).thenReturn("bucket");
        List<NodeInfo> nodeInfos = new ArrayList<NodeInfo>();
        Map<String, Integer> ports = new HashMap<String, Integer>();
        ports.put("direct", 11210);
        nodeInfos.add(new DefaultNodeInfo(null, "localhost:8091", ports));
        when(bucketConfig.nodes()).thenReturn(nodeInfos);
        Map<String, BucketConfig> bucketConfigs = new HashMap<String, BucketConfig>();
        bucketConfigs.put("bucket", bucketConfig);

        when(clusterConfig.bucketConfigs()).thenReturn(bucketConfigs);

        ByteBuf content = Unpooled.copiedBuffer("{\"config\": true}", CharsetUtil.UTF_8);
        when(cluster.send(any(GetBucketConfigRequest.class))).thenReturn(Observable.just(
            (CouchbaseResponse) new GetBucketConfigResponse(
                ResponseStatus.SUCCESS, KeyValueStatus.SUCCESS.code(),
                "bucket",
                content,
                NetworkAddress.localhost()
            )
        ));

        refresher.refresh(clusterConfig);

        Thread.sleep(200);

        verify(provider, times(1)).proposeBucketConfig("bucket", "{\"config\": true}");
        assertEquals(0, content.refCnt());
    }

    @Test
    public void shouldNotRefreshWithInvalidClusterConfig() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);
        CarrierRefresher refresher = new CarrierRefresher(ENVIRONMENT, cluster);
        refresher.registerBucket("bucket", "");
        ConfigurationProvider provider = mock(ConfigurationProvider.class);
        refresher.provider(provider);

        ClusterConfig clusterConfig = mock(ClusterConfig.class);
        BucketConfig bucketConfig = mock(BucketConfig.class);
        when(bucketConfig.name()).thenReturn("bucket");
        List<NodeInfo> nodeInfos = new ArrayList<NodeInfo>();

        Map<String, Integer> ports = new HashMap<String, Integer>();
        ports.put("direct", 11210);
        nodeInfos.add(new DefaultNodeInfo(null, "localhost:8091", ports));
        when(bucketConfig.nodes()).thenReturn(nodeInfos);
        Map<String, BucketConfig> bucketConfigs = new HashMap<String, BucketConfig>();
        bucketConfigs.put("bucket", bucketConfig);

        when(clusterConfig.bucketConfigs()).thenReturn(bucketConfigs);

        ByteBuf content = Unpooled.copiedBuffer("", CharsetUtil.UTF_8);
        when(cluster.send(any(GetBucketConfigRequest.class))).thenReturn(Observable.just(
            (CouchbaseResponse) new GetBucketConfigResponse(
                ResponseStatus.FAILURE, KeyValueStatus.ERR_NOT_FOUND.code(),
                "bucket",
                content,
                NetworkAddress.localhost()
            )
        ));

        refresher.refresh(clusterConfig);

        Thread.sleep(200);

        verify(provider, never()).proposeBucketConfig("bucket", "");
        assertEquals(0, content.refCnt());
    }

    @Test
    public void shouldFallbackToNextOnRefreshWhenFirstFails() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);
        CarrierRefresher refresher = new CarrierRefresher(ENVIRONMENT, cluster);
        refresher.registerBucket("bucket", "");
        ConfigurationProvider provider = mock(ConfigurationProvider.class);
        refresher.provider(provider);

        ClusterConfig clusterConfig = mock(ClusterConfig.class);
        BucketConfig bucketConfig = mock(BucketConfig.class);
        when(bucketConfig.name()).thenReturn("bucket");
        List<NodeInfo> nodeInfos = new ArrayList<NodeInfo>();

        Map<String, Integer> ports = new HashMap<String, Integer>();
        ports.put("direct", 11210);
        nodeInfos.add(new DefaultNodeInfo(null, "1.2.3.4:8091", ports));
        nodeInfos.add(new DefaultNodeInfo(null, "2.3.4.5:8091", ports));
        when(bucketConfig.nodes()).thenReturn(nodeInfos);
        Map<String, BucketConfig> bucketConfigs = new HashMap<String, BucketConfig>();
        bucketConfigs.put("bucket", bucketConfig);

        when(clusterConfig.bucketConfigs()).thenReturn(bucketConfigs);

        ByteBuf content = Unpooled.copiedBuffer("{\"config\": true}", CharsetUtil.UTF_8);
        Observable<CouchbaseResponse> goodResponse = Observable.just(
            (CouchbaseResponse) new GetBucketConfigResponse(
                ResponseStatus.SUCCESS, KeyValueStatus.SUCCESS.code(),
                "bucket",
                content,
                NetworkAddress.create("1.2.3.4")
            )
        );
        Observable<CouchbaseResponse> badResponse = Observable.error(new CouchbaseException("Woops.."));
        when(cluster.send(any(GetBucketConfigRequest.class))).thenReturn(badResponse, goodResponse);

        refresher.refresh(clusterConfig);

        Thread.sleep(1500);

        verify(provider, times(1)).proposeBucketConfig("bucket", "{\"config\": true}");
        assertEquals(0, content.refCnt());
    }

    @Test
    public void shouldFallbackToNextOnPollWhenFirstFails() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);
        ConfigurationProvider provider = mock(ConfigurationProvider.class);
        BucketConfig config = mock(BucketConfig.class);

        CarrierRefresher refresher = new CarrierRefresher(ENVIRONMENT, cluster);
        refresher.provider(provider);

        when(config.name()).thenReturn("bucket");
        List<NodeInfo> nodeInfos = new ArrayList<NodeInfo>();

        Map<String, Integer> ports = new HashMap<String, Integer>();
        ports.put("direct", 11210);
        nodeInfos.add(new DefaultNodeInfo(null, "1.2.3.4:8091", ports));
        nodeInfos.add(new DefaultNodeInfo(null, "2.3.4.5:8091", ports));
        when(config.nodes()).thenReturn(nodeInfos);

        ByteBuf content = Unpooled.copiedBuffer("{\"config\": true}", CharsetUtil.UTF_8);
        Observable<CouchbaseResponse> goodResponse = Observable.just((CouchbaseResponse) new GetBucketConfigResponse(
            ResponseStatus.SUCCESS, KeyValueStatus.SUCCESS.code(),
            "bucket",
            content,
            NetworkAddress.create("1.2.3.4")
        ));
        Observable<CouchbaseResponse> badResponse = Observable.error(new CouchbaseException("Failure"));
        when(cluster.send(any(GetBucketConfigRequest.class))).thenReturn(badResponse, goodResponse);
        refresher.markTainted(config);

        Thread.sleep(1500);

        verify(provider, times(1)).proposeBucketConfig("bucket", "{\"config\": true}");
        assertEquals(0, content.refCnt());
    }

    @Test
    public void shouldIgnoreNodeWithoutKVServiceEnabled() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);
        ConfigurationProvider provider = mock(ConfigurationProvider.class);
        BucketConfig config = mock(BucketConfig.class);

        CarrierRefresher refresher = new CarrierRefresher(ENVIRONMENT, cluster);
        refresher.provider(provider);

        when(config.name()).thenReturn("bucket");
        List<NodeInfo> nodeInfos = new ArrayList<NodeInfo>();

        Map<String, Integer> ports = new HashMap<String, Integer>();
        ports.put("direct", 11210);
        nodeInfos.add(new DefaultNodeInfo(null, "1.2.3.4:8091", ports));
        nodeInfos.add(new DefaultNodeInfo(null, "6.7.8.9:8091", new HashMap<String, Integer>()));
        nodeInfos.add(new DefaultNodeInfo(null, "2.3.4.5:8091", ports));
        when(config.nodes()).thenReturn(nodeInfos);

        ByteBuf content = Unpooled.copiedBuffer("{\"config\": true}", CharsetUtil.UTF_8);
        Observable<CouchbaseResponse> goodResponse = Observable.just((CouchbaseResponse) new GetBucketConfigResponse(
                ResponseStatus.SUCCESS, KeyValueStatus.SUCCESS.code(),
                "bucket",
                content,
                NetworkAddress.create("1.2.3.4")
        ));
        Observable<CouchbaseResponse> badResponse = Observable.error(new CouchbaseException("Failure"));
        when(cluster.send(any(GetBucketConfigRequest.class))).thenReturn(badResponse, goodResponse);
        refresher.markTainted(config);

        Thread.sleep(1500);

        verify(provider, times(1)).proposeBucketConfig("bucket", "{\"config\": true}");
        assertEquals(0, content.refCnt());
    }

    @Test
    public void shouldFreeResourcesAtShutdown() {
        ClusterFacade cluster = mock(ClusterFacade.class);
        ConfigurationProvider provider = mock(ConfigurationProvider.class);
        CarrierRefresher refresher = new CarrierRefresher(ENVIRONMENT, cluster);
        refresher.provider(provider);

        assertFalse(refresher.pollerSubscription().isUnsubscribed());
        refresher.shutdown().toBlocking().single();
        assertTrue(refresher.pollerSubscription().isUnsubscribed());
    }

    @Test
    public void shouldNotPollBelowFloor() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);
        CarrierRefresher refresher = new CarrierRefresher(ENVIRONMENT, cluster);
        refresher.registerBucket("bucket", "");
        ConfigurationProvider provider = mock(ConfigurationProvider.class);
        refresher.provider(provider);

        ClusterConfig clusterConfig = mock(ClusterConfig.class);
        BucketConfig bucketConfig = mock(BucketConfig.class);
        when(bucketConfig.name()).thenReturn("bucket");
        List<NodeInfo> nodeInfos = new ArrayList<NodeInfo>();
        Map<String, Integer> ports = new HashMap<String, Integer>();
        ports.put("direct", 11210);
        nodeInfos.add(new DefaultNodeInfo(null, "localhost:8091", ports));
        when(bucketConfig.nodes()).thenReturn(nodeInfos);
        Map<String, BucketConfig> bucketConfigs = new HashMap<String, BucketConfig>();
        bucketConfigs.put("bucket", bucketConfig);

        when(clusterConfig.bucketConfigs()).thenReturn(bucketConfigs);

        final List<Long> invocationTimings = Collections.synchronizedList(new ArrayList<Long>());
        when(cluster.send(any(GetBucketConfigRequest.class))).thenAnswer(new Answer<Observable<CouchbaseResponse>>() {
            @Override
            public Observable<CouchbaseResponse> answer(InvocationOnMock invocation) throws Throwable {
                invocationTimings.add(System.nanoTime());
                return Observable.just(
                        (CouchbaseResponse) new GetBucketConfigResponse(
                                ResponseStatus.SUCCESS, KeyValueStatus.SUCCESS.code(),
                                "bucket",
                                Unpooled.copiedBuffer("{\"config\": true}", CharsetUtil.UTF_8),
                                NetworkAddress.localhost()
                        )
                );
            }
        });

        int attempts = 400;
        for (int i = 0; i < attempts; i++) {
            refresher.refresh(clusterConfig);
            Thread.sleep(2);
        }

        Thread.sleep(200);

        // There is a little bit of flakiness going on in those tests, so hardening them a bit to
        // make sure most of the intervals are fine. Close enough for our purposes.
        long lastCall = invocationTimings.get(0);
        int good = 0;
        int bad = 0;
        for (int i = 1; i < invocationTimings.size(); i++) {
            if ((invocationTimings.get(i) - lastCall) >= DefaultCoreEnvironment.CONFIG_POLL_FLOOR_INTERVAL) {
                good++;
            } else {
                bad++;
            }
            lastCall = invocationTimings.get(i);
        }
        // Make sure we got more calls over the 10ms period than below.
        assertTrue(good > bad);
    }

    @Test
    public void shouldShiftNodeList() {
        ClusterFacade cluster = mock(ClusterFacade.class);
        CarrierRefresher refresher = new CarrierRefresher(ENVIRONMENT, cluster);

        List<Integer> list = new ArrayList<Integer>(Arrays.asList(0, 1, 2, 3, 4));
        refresher.shiftNodeList(list);
        assertEquals(Arrays.asList(0, 1, 2, 3, 4), list); // shift by 0

        list = new ArrayList<Integer>(Arrays.asList(0, 1, 2, 3, 4));
        refresher.shiftNodeList(list);
        assertEquals(Arrays.asList(1, 2, 3, 4, 0), list); // shift by 1

        list = new ArrayList<Integer>(Arrays.asList(0, 1, 2, 3, 4));
        refresher.shiftNodeList(list);
        assertEquals(Arrays.asList(2, 3, 4, 0, 1), list); // shift by 2

        list = new ArrayList<Integer>(Arrays.asList(0, 1, 2, 3, 4));
        refresher.shiftNodeList(list);
        assertEquals(Arrays.asList(3, 4, 0, 1, 2), list); // shift by 3

        list = new ArrayList<Integer>(Arrays.asList(0, 1, 2, 3, 4));
        refresher.shiftNodeList(list);
        assertEquals(Arrays.asList(4, 0, 1, 2, 3), list); // shift by 4

        list = new ArrayList<Integer>(Arrays.asList(0, 1, 2, 3, 4));
        refresher.shiftNodeList(list);
        assertEquals(Arrays.asList(0, 1, 2, 3, 4), list); // shift by 0

        list = new ArrayList<Integer>(Arrays.asList(0, 1, 2, 3, 4));
        refresher.shiftNodeList(list);
        assertEquals(Arrays.asList(1, 2, 3, 4, 0), list); // shift by 1
    }

    @Test
    public void shouldUseShiftedNodeListOnTaintedPolling() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);
        final CarrierRefresher refresher = new CarrierRefresher(ENVIRONMENT, cluster);
        refresher.registerBucket("bucket", "");
        ConfigurationProvider provider = mock(ConfigurationProvider.class);
        refresher.provider(provider);

        ClusterConfig clusterConfig = mock(ClusterConfig.class);
        BucketConfig bucketConfig = mock(BucketConfig.class);
        when(bucketConfig.name()).thenReturn("bucket");
        List<NodeInfo> nodeInfos = new ArrayList<NodeInfo>();

        Map<String, Integer> ports = new HashMap<String, Integer>();
        ports.put("direct", 11210);
        nodeInfos.add(new DefaultNodeInfo(null, "1.2.3.4:8091", ports));
        nodeInfos.add(new DefaultNodeInfo(null, "2.3.4.5:8091", ports));
        when(bucketConfig.nodes()).thenReturn(nodeInfos);
        Map<String, BucketConfig> bucketConfigs = new HashMap<String, BucketConfig>();
        bucketConfigs.put("bucket", bucketConfig);

        when(clusterConfig.bucketConfigs()).thenReturn(bucketConfigs);

        final List<String> nodesRequested = Collections.synchronizedList(new ArrayList<String>());
        when(cluster.send(any(GetBucketConfigRequest.class))).thenAnswer(new Answer<Observable<GetBucketConfigResponse>>() {
            @Override
            public Observable<GetBucketConfigResponse> answer(InvocationOnMock invocation) throws Throwable {
                GetBucketConfigRequest request = (GetBucketConfigRequest) invocation.getArguments()[0];
                nodesRequested.add(request.hostname().address());
                return Observable.just(
                        new GetBucketConfigResponse(
                                ResponseStatus.SUCCESS, KeyValueStatus.SUCCESS.code(),
                                "bucket",
                                Unpooled.copiedBuffer("{\"config\": true}", CharsetUtil.UTF_8),
                                NetworkAddress.localhost()
                        )
                );
            }
        });

        refresher.markTainted(bucketConfig);
        Thread.sleep(3500);
        refresher.markUntainted(bucketConfig);
        Thread.sleep(500);

        assertEquals("1.2.3.4", nodesRequested.get(0));
        assertEquals("2.3.4.5", nodesRequested.get(1));
        assertEquals("1.2.3.4", nodesRequested.get(2));
    }

    @Test
    public void shouldUseShiftedNodeListOnRefreshPolling() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);
        final CarrierRefresher refresher = new CarrierRefresher(ENVIRONMENT, cluster);
        refresher.registerBucket("bucket", "");
        ConfigurationProvider provider = mock(ConfigurationProvider.class);
        refresher.provider(provider);

        ClusterConfig clusterConfig = mock(ClusterConfig.class);
        BucketConfig bucketConfig = mock(BucketConfig.class);
        when(bucketConfig.name()).thenReturn("bucket");
        List<NodeInfo> nodeInfos = new ArrayList<NodeInfo>();

        Map<String, Integer> ports = new HashMap<String, Integer>();
        ports.put("direct", 11210);
        nodeInfos.add(new DefaultNodeInfo(null, "1.2.3.4:8091", ports));
        nodeInfos.add(new DefaultNodeInfo(null, "2.3.4.5:8091", ports));
        when(bucketConfig.nodes()).thenReturn(nodeInfos);
        Map<String, BucketConfig> bucketConfigs = new HashMap<String, BucketConfig>();
        bucketConfigs.put("bucket", bucketConfig);

        when(clusterConfig.bucketConfigs()).thenReturn(bucketConfigs);

        final List<String> nodesRequested = Collections.synchronizedList(new ArrayList<String>());
        when(cluster.send(any(GetBucketConfigRequest.class))).thenAnswer(new Answer<Observable<GetBucketConfigResponse>>() {
            @Override
            public Observable<GetBucketConfigResponse> answer(InvocationOnMock invocation) throws Throwable {
                GetBucketConfigRequest request = (GetBucketConfigRequest) invocation.getArguments()[0];
                nodesRequested.add(request.hostname().address());
                return Observable.just(
                        new GetBucketConfigResponse(
                                ResponseStatus.SUCCESS, KeyValueStatus.SUCCESS.code(),
                                "bucket",
                                Unpooled.copiedBuffer("{\"config\": true}", CharsetUtil.UTF_8),
                                NetworkAddress.localhost()
                        )
                );
            }
        });

        refresher.refresh(clusterConfig);
        Thread.sleep(500);
        refresher.refresh(clusterConfig);
        Thread.sleep(500);
        refresher.refresh(clusterConfig);
        Thread.sleep(500);
        refresher.refresh(clusterConfig);
        Thread.sleep(500);

        verify(provider, times(4)).proposeBucketConfig("bucket", "{\"config\": true}");
        assertEquals("1.2.3.4", nodesRequested.get(0));
        assertEquals("2.3.4.5", nodesRequested.get(1));
        assertEquals("1.2.3.4", nodesRequested.get(2));
        assertEquals("2.3.4.5", nodesRequested.get(3));
    }

    @Test
    public void shouldEnforcePollFloorPerBucket() throws Exception {
        CoreEnvironment env = mock(CoreEnvironment.class);
        when(env.sslEnabled()).thenReturn(false);
        when(env.scheduler()).thenReturn(new CoreScheduler(4));
        ClusterFacade clusterFacade = mock(ClusterFacade.class);
        when(clusterFacade.send(any(CouchbaseRequest.class))).thenReturn(Observable.<CouchbaseResponse>never());
        CarrierRefresher refresher = new CarrierRefresher(env, clusterFacade);
        refresher.registerBucket("bucket1", "password");
        refresher.registerBucket("bucket2", "password");

        List<NodeInfo> nodeInfos = new ArrayList<NodeInfo>();
        Map<String, Integer> ports = new HashMap<String, Integer>();
        ports.put("direct", 11210);
        nodeInfos.add(new DefaultNodeInfo(null, "1.2.3.4:8091", ports));
        nodeInfos.add(new DefaultNodeInfo(null, "2.3.4.5:8091", ports));


        ClusterConfig cc = mock(ClusterConfig.class);
        Map<String, BucketConfig> bcs = new HashMap<String, BucketConfig>();
        BucketConfig b1 = mock(BucketConfig.class);
        when(b1.name()).thenReturn("bucket1");
        when(b1.nodes()).thenReturn(nodeInfos);
        BucketConfig b2 = mock(BucketConfig.class);
        when(b2.name()).thenReturn("bucket2");
        when(b2.nodes()).thenReturn(nodeInfos);
        bcs.put("bucket1", b1);
        bcs.put("bucket2", b2);
        when(cc.bucketConfigs()).thenReturn(bcs);

        refresher.refresh(cc);

        Thread.sleep(500);

        verify(clusterFacade, times(2)).send(any(CouchbaseRequest.class));
    }
}
