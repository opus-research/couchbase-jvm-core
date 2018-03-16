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
package com.couchbase.client.core.node;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.ResponseHandler;
import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.internal.AddServiceRequest;
import com.couchbase.client.core.message.internal.RemoveServiceRequest;
import com.couchbase.client.core.message.internal.SignalFlush;
import com.couchbase.client.core.service.Service;
import com.couchbase.client.core.service.ServiceFactory;
import com.couchbase.client.core.state.AbstractStateMachine;
import com.couchbase.client.core.state.LifecycleState;
import com.lmax.disruptor.RingBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.FuncN;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * The general implementation of a {@link Node}.
 *
 * A {@link Node} manages one or more {@link Service}s. When a node gets connected, all currently configured
 * {@link Service}s are connected. Those can and will also be added and removed on demand. On disconnect, all
 * services will be shut down asynchronously and then the node is determined to be shutdown.
 *
 * A {@link Node}s states is composed exclusively of the underlying {@link Service} states.
 *
 * @author Michael Nitschinger
 * @since 2.0
 */
public class CouchbaseNode extends AbstractStateMachine<LifecycleState> implements Node {

    /**
     * The logger to use for all nodes.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(Node.class);

    /**
     * The hostname or IP address of the node.
     */
    private final InetAddress hostname;

    /**
     * The environment to use.
     */
    private final Environment environment;

    /**
     * The {@link ResponseEvent} {@link RingBuffer}.
     */
    private final RingBuffer<ResponseEvent> responseBuffer;

    /**
     * A registry containing all of the services associated with one or more buckets.
     */
    private final ServiceRegistry serviceRegistry;

    private final List<Observable<LifecycleState>> serviceStates;

    private volatile Subscription stateSubscription;

    public CouchbaseNode(final InetAddress hostname, final Environment environment,
        final RingBuffer<ResponseEvent> responseBuffer) {
        this(hostname, new DefaultServiceRegistry(), environment, responseBuffer);
    }

    CouchbaseNode(final InetAddress hostname, ServiceRegistry registry, final Environment environment,
        final RingBuffer<ResponseEvent> responseBuffer) {
        super(LifecycleState.DISCONNECTED);
        this.hostname = hostname;
        this.serviceRegistry = registry;
        this.environment = environment;
        this.responseBuffer = responseBuffer;
        this.serviceStates = Collections.synchronizedList(new ArrayList<Observable<LifecycleState>>());

        stateSubscription = Observable.combineLatest(serviceStates, new FuncN<LifecycleState>() {
            @Override
            public LifecycleState call(Object... args) {
                LifecycleState[] states = Arrays.copyOf(args, args.length, LifecycleState[].class);
                return calculateStateFrom(Arrays.asList(states));
            }
        }).subscribe(new Action1<LifecycleState>() {
            @Override
            public void call(LifecycleState state) {
                if (state == state()) {
                    return;
                }

                LifecycleState before = state();
                transitionState(state);

                if (state() == LifecycleState.CONNECTED) {
                    LOGGER.info("Connected to Node " + hostname);
                } else if ((before == LifecycleState.CONNECTED || before == LifecycleState.DISCONNECTING)
                    && state() == LifecycleState.DISCONNECTED) {
                    LOGGER.info("Disconnected from Node " + hostname);
                }


            }
        });

    }

    @Override
    public void send(final CouchbaseRequest request) {
        if (request instanceof SignalFlush) {
            for (Service service : serviceRegistry.services()) {
                service.send(request);
            }
        } else {
            Service service = serviceRegistry.locate(request);
            if (service == null) {
                responseBuffer.publishEvent(ResponseHandler.RESPONSE_TRANSLATOR, request, request.observable());
            } else {
                service.send(request);
            }
        }
    }

    @Override
    public InetAddress hostname() {
        return hostname;
    }

    @Override
    public Observable<LifecycleState> connect() {
        return Observable.from(serviceRegistry.services()).flatMap(new Func1<Service, Observable<LifecycleState>>() {
            @Override
            public Observable<LifecycleState> call(final Service service) {
                return service.connect();
            }
        }).toList().map(new Func1<List<LifecycleState>, LifecycleState>() {
            @Override
            public LifecycleState call(final List<LifecycleState> serviceStates) {
                return state();
            }
        });
    }

    @Override
    public Observable<LifecycleState> disconnect() {
        return Observable
            .from(serviceRegistry.services())
            .flatMap(new Func1<Service, Observable<LifecycleState>>() {
                @Override
                public Observable<LifecycleState> call(final Service service) {
                    return service.disconnect();
                }
            })
            .toList()
            .map(new Func1<List<LifecycleState>, LifecycleState>() {
                @Override
                public LifecycleState call(final List<LifecycleState> serviceStates) {
                    return state();
                }
            });
    }

    @Override
    public Observable<Service> addService(final AddServiceRequest request) {
        Service addedService = serviceRegistry.serviceBy(request.type(), request.bucket());
        if (addedService != null) {
            return Observable.from(addedService);
        }

        final Service service = ServiceFactory.create(
            request.hostname().getHostName(),
            request.bucket(),
            request.password(),
            request.port(),
            environment,
            request.type(),
            responseBuffer
        );

        serviceRegistry.addService(service, request.bucket());
        serviceStates.add(service.states());
        return service.connect().map(new Func1<LifecycleState, Service>() {
            @Override
            public Service call(LifecycleState state) {
                return service;
            }
        });
    }

    @Override
    public Observable<Service> removeService(final RemoveServiceRequest request) {
        Service service = serviceRegistry.serviceBy(request.type(), request.bucket());
        serviceRegistry.removeService(service, request.bucket());
        if (stateSubscription != null) {
            stateSubscription.unsubscribe();
        }
        serviceStates.remove(service.states());
        return Observable.from(service);
    }

    /**
     * Calculates the states for a {@link CouchbaseNode} based on the given {@link Service} states.
     *
     * The rules are as follows in strict order:
     *   1) No Service States -> Disconnected
     *   2) All Services Connected -> Connected
     *   3) At least one Service Connected -> Degraded
     *   4) At least one Service Connecting -> Connecting
     *   5) At least one Service Disconnecting -> Disconnecting
     *   6) Otherwise -> Disconnected
     *
     * @param serviceStates the input service states.
     * @return the output node states.
     */
    private static LifecycleState calculateStateFrom(final List<LifecycleState> serviceStates) {
        if (serviceStates.isEmpty()) {
            return LifecycleState.DISCONNECTED;
        }
        int connected = 0;
        int connecting = 0;
        int disconnecting = 0;
        for (LifecycleState serviceState : serviceStates) {
            switch (serviceState) {
                case CONNECTED:
                    connected++;
                    break;
                case CONNECTING:
                    connecting++;
                    break;
                case DISCONNECTING:
                    disconnecting++;
                    break;
            }
        }
        if (serviceStates.size() == connected) {
            return LifecycleState.CONNECTED;
        } else if (connected > 0) {
            return LifecycleState.DEGRADED;
        } else if (connecting > 0) {
            return LifecycleState.CONNECTING;
        } else if (disconnecting > 0) {
            return LifecycleState.DISCONNECTING;
        } else {
            return LifecycleState.DISCONNECTED;
        }
    }

}
