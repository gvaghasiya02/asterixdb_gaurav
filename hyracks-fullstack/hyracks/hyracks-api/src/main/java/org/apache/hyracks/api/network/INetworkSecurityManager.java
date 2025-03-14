/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.api.network;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

public interface INetworkSecurityManager {

    /**
     * Creates a new ssl context based on the current configuration of this {@link INetworkSecurityManager}
     *
     * @return a new ssl context
     */
    SSLContext newSSLContext(boolean clientMode);

    /**
     * Creates a new ssl engine based on the current configuration of this {@link INetworkSecurityManager}
     *
     * @return a new ssl engine
     */
    SSLEngine newSSLEngine(boolean clientMode);

    /**
     * Sets the configuration to be used for this {@link INetworkSecurityManager}
     *
     * @param config
     */
    void setConfiguration(INetworkSecurityConfig config);

    /**
     * Gets the socket channel factory
     *
     * @return the socket channel factory
     */
    ISocketChannelFactory getSocketChannelFactory();

    /**
     * Gets the current configuration of this {@link INetworkSecurityManager}
     *
     * @return the current configuration
     */
    INetworkSecurityConfig getConfiguration();
}
