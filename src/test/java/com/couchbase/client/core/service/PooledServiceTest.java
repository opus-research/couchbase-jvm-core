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
package com.couchbase.client.core.service;

import com.couchbase.client.core.RequestCancelledException;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.internal.SignalFlush;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.service.strategies.SelectionStrategy;
import com.couchbase.client.core.state.LifecycleState;
import org.junit.BeforeClass;
import org.junit.Test;
import rx.functions.Func1;
import rx.observers.TestSubscriber;
import rx.subjects.AsyncSubject;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link PooledService}s.
 *
 * @author Michael Nitschinger
 * @since 1.4.2
 */
public class PooledServiceTest {

    private static volatile CoreEnvironment ENV;

    @BeforeClass
    public static void setup() {
        ENV = mock(CoreEnvironment.class);
        when(ENV.retryStrategy()).thenReturn(FailFastRetryStrategy.INSTANCE);
    }

    @Test(expected =  IllegalArgumentException.class)
    public void shouldFailIfMinIsNegative() {
        new MockedService(ServiceType.BINARY,
            EndpointFactoryMock.simple(null, null), -1, 2, null);
    }

    @Test(expected =  IllegalArgumentException.class)
    public void shouldFailIfMaxIsNegative() {
        new MockedService(ServiceType.BINARY,
            EndpointFactoryMock.simple(null, null), 0, -2, null);
    }

    @Test(expected =  IllegalArgumentException.class)
    public void shouldFailIfMaxIs0() {
        new MockedService(ServiceType.BINARY,
            EndpointFactoryMock.simple(null, null), 0, 0, null);
    }

    @Test(expected =  IllegalArgumentException.class)
    public void shouldFailIfMaxGreaterThanMinEndpoints() {
        new MockedService(ServiceType.BINARY,
            EndpointFactoryMock.simple(null, null), 3, 2, null);
    }

    @Test
    public void shouldProperlyExposeMapping() {
        MockedService ms =  new MockedService(ServiceType.BINARY,
                EndpointFactoryMock.simple(null, null), 2, 2, null);
        assertEquals(ServiceType.BINARY, ms.type());
        assertEquals(ServiceType.BINARY.mapping(), ms.mapping());
    }

    @Test
    public void shouldBeIdleOnBootIfMinEndpointsIs0() {
        MockedService ms = new MockedService(ServiceType.CONFIG,
            EndpointFactoryMock.simple(null, null), 0, 10, null);

        assertEquals(ServiceType.CONFIG, ms.type());
        assertEquals(LifecycleState.IDLE, ms.state());

        LifecycleState afterConnectState = ms.connect().toBlocking().single();
        assertEquals(LifecycleState.IDLE, afterConnectState);
    }

    @Test
    public void shouldBeDisconnectedOnBootIfMinEndpointsIsGt0() {
        MockedService ms = new MockedService(ServiceType.BINARY,
            EndpointFactoryMock.simple(null, null), 3, 10, null);
        assertEquals(ServiceType.BINARY, ms.type());
        assertEquals(LifecycleState.DISCONNECTED, ms.state());
    }

    @Test
    public void shouldSuccessfullyBootstrapMinEndpoints() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);
        ef.onConnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.CONNECTING;
            }
        });
        MockedService ms = new MockedService(ServiceType.BINARY, ef, 3, 4, null);

        LifecycleState afterConnectState = ms.connect().toBlocking().single();
        assertEquals(LifecycleState.CONNECTING, afterConnectState);

        assertEquals(LifecycleState.CONNECTING, ms.state());
        ef.advance(0, LifecycleState.CONNECTED);
        assertEquals(LifecycleState.DEGRADED, ms.state());
        ef.advance(1, LifecycleState.CONNECTED);
        assertEquals(LifecycleState.DEGRADED, ms.state());
        ef.advance(2, LifecycleState.CONNECTED);
        assertEquals(LifecycleState.CONNECTED, ms.state());

        assertEquals(3, ef.endpointCount());
        assertEquals(3, ef.endpointConnectCalled());
    }

    @Test
    public void shouldIgnoreConnectIfConnecting() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);
        ef.onConnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.CONNECTING;
            }
        });
        MockedService ms = new MockedService(ServiceType.BINARY, ef, 1, 1, null);

        LifecycleState afterConnectState = ms.connect().toBlocking().single();
        assertEquals(LifecycleState.CONNECTING, afterConnectState);
        assertEquals(1, ef.endpointCount());

        afterConnectState = ms.connect().toBlocking().single();
        assertEquals(LifecycleState.CONNECTING, afterConnectState);
        assertEquals(1, ef.endpointCount());
        assertEquals(1, ef.endpointConnectCalled());
    }

    @Test
    public void shouldIgnoreConnectIfConnected() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);
        ef.onConnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.CONNECTING;
            }
        });
        MockedService ms = new MockedService(ServiceType.BINARY, ef, 1, 1, null);

        LifecycleState afterConnectState = ms.connect().toBlocking().single();
        assertEquals(LifecycleState.CONNECTING, afterConnectState);
        assertEquals(1, ef.endpointCount());

        ef.advance(0, LifecycleState.CONNECTED);

        afterConnectState = ms.connect().toBlocking().single();
        assertEquals(LifecycleState.CONNECTED, afterConnectState);
        assertEquals(1, ef.endpointCount());
        assertEquals(1, ef.endpointConnectCalled());
    }

    @Test
    public void shouldDisconnectIfInstructed() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);
        ef.onConnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.CONNECTING;
            }
        });
        ef.onDisconnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.DISCONNECTING;
            }
        });
        MockedService ms = new MockedService(ServiceType.BINARY, ef, 4, 4, null);
        LifecycleState afterConnectState = ms.connect().toBlocking().single();
        assertEquals(LifecycleState.CONNECTING, afterConnectState);
        assertEquals(4, ef.endpointCount());

        ef.advanceAll(LifecycleState.CONNECTED);

        LifecycleState stateAfterDisconnect = ms.disconnect().toBlocking().single();
        assertEquals(LifecycleState.DISCONNECTED, stateAfterDisconnect);
        assertEquals(4, ef.endpointDisconnectCalled());
    }

    @Test
    public void shouldIgnoreDisconnectIfDisconnecting() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);
        ef.onConnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.CONNECTING;
            }
        });
        ef.onDisconnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.DISCONNECTING;
            }
        });
        MockedService ms = new MockedService(ServiceType.BINARY, ef, 4, 4, null);
        LifecycleState afterConnectState = ms.connect().toBlocking().single();
        assertEquals(LifecycleState.CONNECTING, afterConnectState);
        assertEquals(4, ef.endpointCount());

        ef.advanceAll(LifecycleState.CONNECTED);

        ef.advance(0, LifecycleState.DISCONNECTING);
        ef.advance(1, LifecycleState.DISCONNECTING);
        ef.advance(2, LifecycleState.DISCONNECTING);
        ef.advance(3, LifecycleState.DISCONNECTING);

        assertEquals(LifecycleState.DISCONNECTING, ms.state());

        LifecycleState stateAfterDisconnect = ms.disconnect().toBlocking().single();
        assertEquals(LifecycleState.DISCONNECTING, stateAfterDisconnect);

        assertEquals(0, ef.endpointDisconnectCalled());

        stateAfterDisconnect = ms.disconnect().toBlocking().single();
        assertEquals(LifecycleState.DISCONNECTING, stateAfterDisconnect);
        assertEquals(0, ef.endpointDisconnectCalled());
    }

    @Test
    public void shouldIgnoreDisconnectIfDisconnected() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(null, null);
        MockedService ms = new MockedService(ServiceType.BINARY, ef, 1, 1, null);
        assertEquals(ServiceType.BINARY, ms.type());
        assertEquals(LifecycleState.DISCONNECTED, ms.state());

        LifecycleState afterDisconnectState = ms.disconnect().toBlocking().single();
        assertEquals(LifecycleState.DISCONNECTED, afterDisconnectState);
        assertEquals(0, ef.endpointDisconnectCalled());
    }

    @Test
    public void shouldGenerateIdentityLogLine() {
        MockedService ms = new MockedService(ServiceType.CONFIG,
            EndpointFactoryMock.simple(ENV, null), 0, 10, null);
        String actual = PooledService.logIdent("hostname", ms);
        assertEquals("[hostname][MockedService]: ", actual);

        actual = PooledService.logIdent(null, ms);
        assertEquals("[null][MockedService]: ", actual);
    }

    @Test
    public void shouldSendToEndpointWhenFixed() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);
        SelectionStrategy ss = mock(SelectionStrategy.class);

        MockedService ms = new MockedService(ServiceType.BINARY, ef, 2, 2, ss);
        ms.connect().toBlocking().single();
        ef.advanceAll(LifecycleState.CONNECTED);

        CouchbaseRequest request = mock(CouchbaseRequest.class);
        when(ss.select(same(request), any(Endpoint[].class))).thenReturn(ef.endpoints().get(0));

        ms.send(request);
        assertEquals(1, ef.endpointSendCalled());
    }

    @Test
    public void shouldBePutIntoRetryWhenFixed() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);
        SelectionStrategy ss = mock(SelectionStrategy.class);

        MockedService ms = new MockedService(ServiceType.BINARY, ef, 2, 2, ss);
        ms.connect().toBlocking().single();
        ef.advanceAll(LifecycleState.CONNECTED);

        CouchbaseRequest request = mock(CouchbaseRequest.class);
        AsyncSubject<CouchbaseResponse> subject = AsyncSubject.create();
        when(request.observable()).thenReturn(subject);
        when(ss.select(same(request), any(Endpoint[].class))).thenReturn(null);

        TestSubscriber<CouchbaseResponse> subscriber = TestSubscriber.create();
        subject.subscribe(subscriber);

        ms.send(request);
        assertEquals(0, ef.endpointSendCalled());
        subscriber.assertError(RequestCancelledException.class);
    }

    @Test
    public void shouldPropagateFlushToAllEndpoints() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);

        MockedService ms = new MockedService(ServiceType.BINARY, ef, 3, 3, null);
        ms.connect().toBlocking().single();
        ef.advanceAll(LifecycleState.CONNECTED);

        ms.send(SignalFlush.INSTANCE);
        assertEquals(3, ef.endpointSendCalled());
    }

    // test: from 0 ..1, from 1..2 with IDLE success, check EP registration and mock calls
    // test: from N to N + 1 from disconnected success, check EP registration and mock calls
    // test: proper rescheduling if socket connect open fails (also way to test unsubscription?), check EP is not registered then

    @Test
    public void shouldSendFromIdleToOne() {

    }

    @Test
    public void shouldSendFromMinPlusOne() {

    }

    @Test
    public void shouldRetryWhenMinEqMax() {

    }

    @Test
    public void shouldRetryWhenSocketOpenFailedOnSend() {

    }

    /**
     * A simple service which can be mocked in all kinds of ways to test the functionality of the
     * pooled service.
     */
    static class MockedService extends PooledService {

        private final ServiceType serviceType;

        MockedService(ServiceType st, EndpointFactoryMock ef, int minEndpoints, int maxEndpoints,
            SelectionStrategy ss) {
            super(ef.getHostname(), ef.getBucket(), ef.getPassword(), ef.getPort(), ef.getEnv(),
                minEndpoints, maxEndpoints, ef.getResponseBuffer(), ef, ss);
            this.serviceType = st;
        }

        @Override
        public ServiceType type() {
            return serviceType;
        }
    }
}