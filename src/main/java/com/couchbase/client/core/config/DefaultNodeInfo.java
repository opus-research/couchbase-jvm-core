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
package com.couchbase.client.core.config;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.utils.NetworkAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Default implementation of {@link NodeInfo}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class DefaultNodeInfo implements NodeInfo {

    private final NetworkAddress hostname;
    private final Map<ServiceType, Integer> directServices;
    private final Map<ServiceType, Integer> sslServices;
    private int configPort;

    /**
     * Creates a new {@link DefaultNodeInfo} with no SSL services.
     *
     * @param viewUri  the URI of the view service.
     * @param hostname the hostname of the node.
     * @param ports    the port list of the node services.
     */
    @JsonCreator
    public DefaultNodeInfo(
        @JsonProperty("couchApiBase") String viewUri,
        @JsonProperty("hostname") String hostname,
        @JsonProperty("ports") Map<String, Integer> ports) {
        if (hostname == null) {
            throw new CouchbaseException(new IllegalArgumentException("NodeInfo hostname cannot be null"));
        }

        try {
            this.hostname = NetworkAddress.create(trimPort(hostname));
        } catch (Exception e) {
            throw new CouchbaseException("Could not analyze hostname from config.", e);
        }
        this.directServices = parseDirectServices(viewUri, ports);
        this.sslServices = new HashMap<ServiceType, Integer>();
    }

    /**
     * Creates a new {@link DefaultNodeInfo} with SSL services.
     *
     * @param hostname the hostname of the node.
     * @param direct   the port list of the direct node services.
     * @param ssl      the port list of the ssl node services.
     */
    public DefaultNodeInfo(NetworkAddress hostname, Map<ServiceType, Integer> direct,
        Map<ServiceType, Integer> ssl) {
        if (hostname == null) {
            throw new CouchbaseException(new IllegalArgumentException("NodeInfo hostname cannot be null"));
        }

        this.hostname = hostname;
        this.directServices = direct;
        this.sslServices = ssl;
    }

    @Override
    public NetworkAddress hostname() {
        return hostname;
    }

    @Override
    public Map<ServiceType, Integer> services() {
        return directServices;
    }

    @Override
    public Map<ServiceType, Integer> sslServices() {
        return sslServices;
    }

    private Map<ServiceType, Integer> parseDirectServices(final String viewUri, final Map<String, Integer> input) {
        Map<ServiceType, Integer> services = new HashMap<ServiceType, Integer>();
        for (Map.Entry<String, Integer> entry : input.entrySet()) {
            String type = entry.getKey();
            Integer port = entry.getValue();
            if (type.equals("direct")) {
                services.put(ServiceType.BINARY, port);
            }
        }
        services.put(ServiceType.CONFIG, configPort);
        if (viewUri != null) {
            services.put(ServiceType.VIEW, URI.create(viewUri).getPort());
        }
        return services;
    }

    private String trimPort(final String hostname) {
        String[] parts = hostname.split(":");
        configPort = Integer.parseInt(parts[parts.length - 1]);

        if (parts.length > 2) {
            // Handle IPv6 syntax
            System.out.println(Arrays.asList(parts));
            String assembledHost = "";
            for (int i = 0; i < parts.length - 1; i++) {
                assembledHost += parts[i];
                if (parts[i].endsWith("]")) {
                    break;
                } else {
                    assembledHost += ":";
                }
            }
            return assembledHost;
        } else {
            // Simple IPv4 Handling
            return parts[0];
        }
    }

    @Override
    public String toString() {
        return "NodeInfo{" + ", hostname=" + hostname + ", configPort="
                + configPort + ", directServices=" + directServices + ", sslServices=" + sslServices + '}';
    }
}
