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

package com.couchbase.client.core.state;

/**
 * **Represents common lifecycle states of components.**
 *
 * The {@link LifecycleState}s are usually combined with the {@link AbstractStateMachine} to build up a state-machine
 * like, observable component that can be subscribed from external components.
 *
 * ![State Transitions](transitions.png)
 *
 * @startuml transitions.png
 *
 *     [*] --> Disconnected
 *     Disconnected --> Connecting
 *     Connecting --> Disconnected
 *     Connecting --> Connected
 *     Connecting --> Degraded
 *     Connected --> Disconnecting
 *     Connected --> Degraded
 *     Degraded --> Connected
 *     Disconnecting -> Disconnected
 *
 * @enduml
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public enum LifecycleState {

    /**
     * The component is currently disconnected.
     */
    DISCONNECTED,

    /**
     * The component is currently connecting or reconnecting.
     */
    CONNECTING,

    /**
     * The component is connected without degradation.
     */
    CONNECTED,

    /**
     * The component is disconnecting.
     */
    DISCONNECTING,

    /**
     * The component is connected, but with service degradation.
     */
    DEGRADED,

    /**
     * The component is idle and has no associated connections to identify.
     *
     * This is most commonly the case with "on demand" services, when no endpoints are
     * registered. In this case "DISCONNECTED" is not the right way to describe its state.
     */
    IDLE

}