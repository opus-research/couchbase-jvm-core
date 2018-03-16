package com.couchbase.client.core.node.locate;

import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.node.Node;
import rx.Observable;

import java.util.Set;

public interface Locator {

    Observable<Node> locate(CouchbaseRequest request, Set<Node> nodes, ClusterConfig config);
}
