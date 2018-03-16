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

package com.couchbase.client.core.tracing;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.CouchbaseCore;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.kv.GetRequest;
import com.couchbase.client.core.message.kv.GetResponse;
import com.uber.jaeger.Configuration;
import io.opentracing.util.GlobalTracer;
import org.junit.Test;

public class JaegerExample {

    @Test
    public void testing() throws Exception {
        GlobalTracer.register(new Configuration(
                "couchbase",
                new Configuration.SamplerConfiguration("const", 1),
                new Configuration.ReporterConfiguration(true, "localhost", 5775, 1000, 10000)
        ).getTracer());

        CoreEnvironment env = DefaultCoreEnvironment.create();
        ClusterFacade core = new CouchbaseCore(env);
        core.send(new SeedNodesRequest("127.0.0.1")).toBlocking().single();
        core.send(new OpenBucketRequest("travel-sample", "Administrator", "password")).toBlocking().single();

        for  (int i = 0; i < 1024; i++) {
            GetRequest request = new GetRequest("airline_"+i, "travel-sample");
            request.span(GlobalTracer.get().buildSpan("getRequest"));
            GetResponse response = core.<GetResponse>send(request).toBlocking().single();
            response.release();
        }

    }
}
