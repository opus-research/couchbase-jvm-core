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

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.env.AbstractServiceConfig;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.internal.SignalFlush;
import com.couchbase.client.core.retry.RetryHelper;
import com.couchbase.client.core.service.strategies.SelectionStrategy;
import com.couchbase.client.core.state.AbstractStateMachine;
import com.couchbase.client.core.state.LifecycleState;
import com.lmax.disruptor.RingBuffer;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func1;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A generic implementation of a service pool.
 *
 * @author Michael Nitschinger
 * @since 1.4.2
 */
public abstract class PooledService extends AbstractStateMachine<LifecycleState> implements Service {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(Service.class);

    private final String hostname;
    private final String bucket;
    private final String password;
    private final int port;
    private final CoreEnvironment env;
    private final int minEndpoints;
    private final int maxEndpoints;
    private final boolean fixedEndpoints;
    private final EndpointStateZipper endpointStates;
    private final RingBuffer<ResponseEvent> responseBuffer;
    private final EndpointFactory endpointFactory;
    private final List<Endpoint> endpoints;
    private final LifecycleState initialState;
    private final SelectionStrategy selectionStrategy;
    private final Object epMutex = new Object();
    private final AbstractServiceConfig serviceConfig;

    /**
     * Pending requests to account for requests waiting for a socket to be connected.
     */
    private volatile int pendingRequests;

    protected PooledService(final String hostname, final String bucket, final String password, final int port,
        final CoreEnvironment env, AbstractServiceConfig serviceConfig,
        final RingBuffer<ResponseEvent> responseBuffer, final EndpointFactory endpointFactory,
        final SelectionStrategy selectionStrategy) {
        super(serviceConfig.minEndpoints() == 0 ? LifecycleState.IDLE : LifecycleState.DISCONNECTED);
        preCheckEndpointSettings(serviceConfig.minEndpoints(), serviceConfig.maxEndpoints());

        this.initialState = serviceConfig.minEndpoints() == 0 ? LifecycleState.IDLE : LifecycleState.DISCONNECTED;
        this.hostname = hostname;
        this.bucket = bucket;
        this.password = password;
        this.port = port;
        this.env = env;
        this.minEndpoints = serviceConfig.minEndpoints();
        this.maxEndpoints = serviceConfig.maxEndpoints();
        this.serviceConfig = serviceConfig;
        this.responseBuffer = responseBuffer;
        this.endpointFactory = endpointFactory;
        this.endpoints = new CopyOnWriteArrayList<Endpoint>();
        this.fixedEndpoints = minEndpoints == maxEndpoints;
        this.selectionStrategy = selectionStrategy;
        this.pendingRequests = 0;
        endpointStates = new EndpointStateZipper(initialState);
        endpointStates.states().subscribe(new Action1<LifecycleState>() {
            @Override
            public void call(LifecycleState lifecycleState) {
                transitionState(lifecycleState);
            }
        });
    }

    private void preCheckEndpointSettings(final int minEndpoints, final int maxEndpoints) {
        if (minEndpoints < 0 || maxEndpoints < 0) {
            throw new IllegalArgumentException("The minEndpoints and maxEndpoints must not be negative");
        }
        if (maxEndpoints == 0) {
            throw new IllegalArgumentException("The maxEndpoints must be greater than 0");
        }
        if (maxEndpoints < minEndpoints) {
            throw new IllegalArgumentException("The maxEndpoints must not be smaller than mindEndpoints");
        }
    }

    @Override
    public Observable<LifecycleState> connect() {
        if (state() == LifecycleState.CONNECTED || state() == LifecycleState.CONNECTING) {
            LOGGER.debug(logIdent(hostname, this) + "Already connected or connecting, skipping connect.");
            return Observable.just(state());
        }
        LOGGER.debug(logIdent(hostname, this) + "Got instructed to connect.");

        synchronized (epMutex) {
            int numToConnect = minEndpoints - endpoints.size();
            if (numToConnect == 0) {
                LOGGER.debug("No endpoints needed to connect, skipping.");
                return Observable.just(state());
            }
            for (int i = 0; i < numToConnect; i++) {
                Endpoint endpoint = endpointFactory.create(hostname, bucket, password, port, env, responseBuffer);
                endpoints.add(endpoint);
                endpointStates.register(endpoint, endpoint);
            }
        }

        return Observable
            .from(endpoints)
            .flatMap(new Func1<Endpoint, Observable<LifecycleState>>() {
                @Override
                public Observable<LifecycleState> call(final Endpoint endpoint) {
                    LOGGER.debug(logIdent(hostname, PooledService.this)
                        + "Connecting Endpoint during Service connect.");
                    return endpoint.connect();
                }
            })
            .lastOrDefault(initialState)
            .map(new Func1<LifecycleState, LifecycleState>() {
                @Override
                public LifecycleState call(final LifecycleState state) {
                    return state();
                }
            });
    }

    @Override
    public Observable<LifecycleState> disconnect() {
        if (state() == LifecycleState.DISCONNECTED || state() == LifecycleState.DISCONNECTING) {
            LOGGER.debug(logIdent(hostname, this) + "Already disconnected or disconnecting, skipping disconnect.");
            return Observable.just(state());
        }
        LOGGER.debug(logIdent(hostname, this) + "Got instructed to disconnect.");

        List<Endpoint> endpoints;
        synchronized (epMutex) {
            endpoints = new ArrayList<Endpoint>(this.endpoints);
            this.endpoints.clear();
        }

        return Observable
            .from(endpoints)
            .flatMap(new Func1<Endpoint, Observable<LifecycleState>>() {
                @Override
                public Observable<LifecycleState> call(Endpoint endpoint) {
                    LOGGER.debug(logIdent(hostname, PooledService.this)
                        + "Disconnecting Endpoint during Service disconnect.");
                    return endpoint.disconnect();
                }
            })
            .lastOrDefault(initialState)
            .map(new Func1<LifecycleState, LifecycleState>() {
                @Override
                public LifecycleState call(final LifecycleState state) {
                    endpointStates.terminate();
                    return state();
                }
            });
    }

    @Override
    public void send(final CouchbaseRequest request) {
        if (request instanceof SignalFlush) {
            sendFlush((SignalFlush) request);
            return;
        }

        // TODO: fixme interface
        Endpoint[] tmp_eps = new Endpoint[endpoints.size()];
        tmp_eps = endpoints.toArray(tmp_eps);


        Endpoint endpoint = tmp_eps.length > 0 ? selectionStrategy.select(request, tmp_eps) : null;

        if (endpoint == null) {
            if (fixedEndpoints || ((endpoints.size() + pendingRequests) >= maxEndpoints)) {
                RetryHelper.retryOrCancel(env, request, responseBuffer);
            } else {
                maybeOpenAndSend(request);
            }
        } else {
            endpoint.send(request);
        }
    }

    /**
     * Helper method to try and open new endpoints as needed and correctly integrate
     * them into the state of the service.
     */
    private void maybeOpenAndSend(final CouchbaseRequest request) {
        pendingRequests++;

        final Endpoint endpoint = endpointFactory.create(
            hostname, bucket, password, port, env, responseBuffer
        );

        final Subscription subscription = whenState(endpoint, LifecycleState.CONNECTED,
            new Action1<LifecycleState>() {
                @Override
                public void call(LifecycleState lifecycleState) {
                    try {
                        endpoint.send(request);
                        endpoint.send(SignalFlush.INSTANCE);

                        synchronized (epMutex) {
                            endpoints.add(endpoint);
                            endpointStates.register(endpoint, endpoint);
                        }
                    } finally {
                        pendingRequests--;
                    }
                }
            }
        );

        endpoint.connect().subscribe(new Subscriber<LifecycleState>() {
            @Override
            public void onCompleted() {
                // ignored on purpose
            }

            @Override
            public void onError(Throwable e) {
                unsubscribeAndRetry(subscription, request);
            }

            @Override
            public void onNext(LifecycleState state) {
                if (state == LifecycleState.DISCONNECTING || state == LifecycleState.DISCONNECTED) {
                    unsubscribeAndRetry(subscription, request);
                }
            }
        });
    }

    /**
     * Helper method to unsubscribe from the subscription and send the request into retry.
     */
    private void unsubscribeAndRetry(final Subscription subscription, final CouchbaseRequest request) {
        if (subscription != null && !subscription.isUnsubscribed()) {
            subscription.unsubscribe();
        }
        pendingRequests--;
        RetryHelper.retryOrCancel(env, request, responseBuffer);
    }

    /**
     * Helper method to send the flush signal to all endpoints.
     *
     * @param signalFlush the flush signal to propagate.
     */
    private void sendFlush(final SignalFlush signalFlush) {
        int length = endpoints.size();
        for (int i = 0; i < length; i++) {
            Endpoint endpoint = endpoints.get(i);
            if (endpoint != null) {
                endpoint.send(signalFlush);
            }
        }
    }

    @Override
    public BucketServiceMapping mapping() {
        return type().mapping();
    }

    /**
     * Simple log helper to give logs a common prefix.
     *
     * @param hostname the address.
     * @param service the service.
     * @return a prefix string for logs.
     */
    static String logIdent(final String hostname, final Service service) {
        return "[" + hostname + "][" + service.getClass().getSimpleName() + "]: ";
    }

    /**
     * Helper method to register a specific action when a state is reached on an
     * endpoint for the first time after subscription.
     *
     * @param endpoint the endpoint to watch.
     * @param wanted the wanted state.
     * @param then the action to execute.
     */
    static Subscription whenState(final Endpoint endpoint, final LifecycleState wanted,
        final Action1<LifecycleState> then) {
        return endpoint
            .states()
            .filter(new Func1<LifecycleState, Boolean>() {
                @Override
                public Boolean call(LifecycleState state) {
                    return state == wanted;
                }
            })
            .take(1)
            .subscribe(then);
    }

    /**
     * Returns the current endpoint list, for testing verification purposes.
     */
    protected List<Endpoint> endpoints() {
        return endpoints;
    }
}