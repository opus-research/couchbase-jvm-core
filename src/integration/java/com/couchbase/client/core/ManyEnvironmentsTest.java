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
package com.couchbase.client.core;

import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.system.TooManyEnvironmentsEvent;
import org.junit.Test;
import rx.functions.Action1;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the event emission if too many environments are created.
 *
 * @author Michael Nitschinger
 * @since 1.3.2
 */
public class ManyEnvironmentsTest {

    /**
     * Note that since this test doesn't run in isolation and other tests heavily alter the number
     * of currently outstanding environments (which is okay), the only thing we can assert is that the
     * message got emitted and that there are more than one environments found.
     */
    @Test
    public void shouldEmitEvent() {
        CoreEnvironment env = DefaultCoreEnvironment.create();

        final AtomicInteger evtCount = new AtomicInteger(0);
        env.eventBus().get().forEach(new Action1<CouchbaseEvent>() {
            @Override
            public void call(CouchbaseEvent couchbaseEvent) {
                if (couchbaseEvent instanceof TooManyEnvironmentsEvent) {
                    evtCount.set(((TooManyEnvironmentsEvent) couchbaseEvent).numEnvs());
                }
            }
        });

        CoreEnvironment env2 = DefaultCoreEnvironment.builder().eventBus(env.eventBus()).build();

        env.shutdown();
        env2.shutdown();

        assertTrue(evtCount.get() > 1);
    }

}
