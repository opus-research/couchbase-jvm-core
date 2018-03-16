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
package com.couchbase.client.core.node;

import com.couchbase.client.core.config.NodeInfo;
import org.junit.Test;

import java.net.InetAddress;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link DefaultMemcachedNodeHashingStrategy}.
 *
 * @author Michael Nitschinger
 * @since 2.3.6
 */
public class DefaultMemcachedNodeHashingStrategyTest {

    @Test
    public void shouldHashNode() throws Exception {
        MemcachedNodeHashingStrategy strategy = new DefaultMemcachedNodeHashingStrategy();

        NodeInfo infoMock = mock(NodeInfo.class);
        when(infoMock.hostname()).thenReturn(InetAddress.getByName("localhost"));
        assertEquals("localhost-0", strategy.nodeHash(infoMock,0));
    }

}