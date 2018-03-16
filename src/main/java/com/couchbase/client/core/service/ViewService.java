package com.couchbase.client.core.service;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.view.ViewEndpoint;
import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.service.strategies.RandomSelectionStrategy;
import com.lmax.disruptor.RingBuffer;

public class ViewService extends AbstractService {

    private static final SelectionStrategy strategy = new RandomSelectionStrategy();
    private static final EndpointFactory factory = new ViewEndpointFactory();

    public ViewService(String hostname, String bucket, String password, int port, Environment env, final RingBuffer<ResponseEvent> responseBuffer) {
        super(hostname, bucket, password, port, env, env.viewServiceEndpoints(), strategy, responseBuffer, factory);
    }

    @Override
    public ServiceType type() {
        return ServiceType.VIEW;
    }

    static class ViewEndpointFactory implements EndpointFactory {
        @Override
        public Endpoint create(String hostname, String bucket, String password, int port, Environment env,
            RingBuffer<ResponseEvent> responseBuffer) {
            return new ViewEndpoint(hostname, bucket, password, port, env, responseBuffer);
        }
    }
}
