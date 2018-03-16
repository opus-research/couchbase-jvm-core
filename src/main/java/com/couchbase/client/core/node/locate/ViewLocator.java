package com.couchbase.client.core.node.locate;

import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.node.Node;

import java.util.Set;

public class ViewLocator implements Locator {

    private long counter = 0;

    @Override
    public Node[] locate(CouchbaseRequest request, Set<Node> nodes, ClusterConfig config) {
        int item = (int) counter % nodes.size();
        int i = 0;
        for (Node node : nodes) {
            if (i++ == item) {
                return new Node[] { node };
            }
        }
        throw new IllegalStateException("Node not found for request" + request);
    }
}
