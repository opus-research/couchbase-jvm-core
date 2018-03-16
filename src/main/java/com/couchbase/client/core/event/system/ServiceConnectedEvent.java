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

package com.couchbase.client.core.event.system;

import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.EventType;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.utils.Events;

import java.net.InetAddress;
import java.util.Map;

/**
 * Event published when a service endpoint is connected
 *
 * @author Subhashni Balakrishnan
 * @since 2.4.2
 */
public class ServiceConnectedEvent implements CouchbaseEvent {

    private final InetAddress host;
    private final ServiceType serviceType;

    public ServiceConnectedEvent(InetAddress host, ServiceType serviceType) {
        this.host = host;
        this.serviceType = serviceType;
    }

    @Override
    public EventType type() {
        return EventType.SYSTEM;
    }

    /**
     * The host address of the disconnected node.
     *
     * @return the inet address of the disconnected node
     */
    public InetAddress host() {
        return host;
    }

    /**
     * Returns the service on this endpoint
     *
     * @return service type
     */
    public ServiceType serviceType() {
        return serviceType;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ServiceConnectedEvent{");
        sb.append("host=").append(host);
        sb.append("service=").append(this.serviceType.toString());
        sb.append('}');
        return sb.toString();
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> result = Events.identityMap(this);
        result.put("host", host().toString());
        result.put("service", serviceType().toString());
        return result;
    }
}