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

package com.couchbase.client.core.env;

import java.security.KeyStore;
import com.couchbase.client.core.endpoint.SSLEngineFactory;

import javax.net.ssl.TrustManagerFactory;

/**
 * A {@link SecureEnvironment} interface defines all methods which environment implementation should have
 * to be accepted by {@link SSLEngineFactory}.
 */
public interface SecureEnvironment {

    /**
     * Identifies if SSL should be enabled.
     *
     * @return true if SSL is enabled, false otherwise.
     */
    boolean sslEnabled();

    /**
     * Identifies the filepath to the ssl keystore.
     *
     * @return the path to the keystore file.
     */
    String sslKeystoreFile();

    /**
     * The password which is used to protect the keystore.
     *
     * @return the keystore password.
     */
    String sslKeystorePassword();

    /**
     * Allows to directly configure a {@link KeyStore}.
     *
     * @return the keystore to use.
     */
    KeyStore sslKeystore();

    /**
     * Identifies the filepath to the ssl TrustManager keystore.
     *
     * Note that this only needs to be used if you need to split up the keystore
     * for certificates and truststore in separate files. If no TrustStore path
     * is used explicitly, the {@link #sslKeystoreFile()} is used. The files is
     * used to load the {@link TrustManagerFactory}.
     *
     * @return the path to the truststore file.
     */
    String sslTruststoreFile();

    /**
     * The password which is used to protect the TrustManager keystore.
     *
     * Only needed if {@link #sslTruststoreFile()} is used.
     *
     * @return the keystore password.
     */
    String sslTruststorePassword();

    /**
     * Allows to directly configure {@link KeyStore} which is used to initialize
     * the {@link TrustManagerFactory} internally. Note that this file only needs
     * to be used if two different keystores should be used, and if not set the
     * regular {@link #sslKeystore()} is used.
     *
     * @return the keystore to use when initializing the {@link TrustManagerFactory}.
     */
    KeyStore sslTruststore();

}
