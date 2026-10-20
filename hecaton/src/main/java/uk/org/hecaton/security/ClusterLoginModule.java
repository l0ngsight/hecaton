/*
 * Copyright (c) 2026, The Hecaton Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.org.hecaton.security;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import java.io.IOException;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Base class for login modules: authenticate in {@link #onLogin()}, naming the identity with
 * {@link #getName()} and adding roles with {@link #addRole(String)}. On commit the subject gets a
 * {@link ClusterIdentityPrincipal}, a {@link ClusterEndpointPrincipal} and the
 * {@link ClusterRolePrincipal}s, unless the options {@code skipIdentity}, {@code skipEndpoint}
 * or {@code skipRole} are {@code true}.
 */
public abstract class ClusterLoginModule implements LoginModule {

    /** Option: do not add the identity principal. */
    public static final String OPTION_SKIP_IDENTITY = "skipIdentity";
    /** Option: do not add the endpoint principal. */
    public static final String OPTION_SKIP_ENDPOINT = "skipEndpoint";
    /** Option: do not add role principals. */
    public static final String OPTION_SKIP_ROLE = "skipRole";

    protected Subject subject;
    protected CallbackHandler callbackHandler;
    protected Map<String, ?> sharedState;
    protected Map<String, ?> options;

    private final List<String> roles = new ArrayList<>();
    private final List<HecatonPrincipal> added = new ArrayList<>();
    private boolean loggedIn;
    private String endpoint;

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState,
                           Map<String, ?> options) {
        this.subject = subject;
        this.callbackHandler = callbackHandler;
        this.sharedState = sharedState;
        this.options = options;
    }

    @Override
    public final boolean login() throws LoginException {
        roles.clear();
        endpoint = getEndpoint();
        loggedIn = onLogin();
        if (loggedIn && getName() == null) {
            throw new FailedLoginException(getClass().getName() + " did not name the authenticated identity");
        }
        return loggedIn;
    }

    /**
     * Authenticates.
     *
     * @return {@code true} if this module authenticated the identity, {@code false} if it is to
     *         be ignored
     * @throws LoginException if authentication failed
     */
    protected abstract boolean onLogin() throws LoginException;

    /** The authenticated identity's name, once {@link #onLogin()} succeeded. */
    protected abstract String getName();

    /** Adds a role to the authenticated identity. */
    protected void addRole(String role) {
        roles.add(role);
    }

    @Override
    public boolean commit() throws LoginException {
        if (!loggedIn) {
            return false;
        }
        if (!getBoolOption(OPTION_SKIP_IDENTITY)) {
            added.add(new ClusterIdentityPrincipal(getName()));
        }
        if (!getBoolOption(OPTION_SKIP_ENDPOINT) && endpoint != null) {
            added.add(new ClusterEndpointPrincipal(endpoint));
        }
        if (!getBoolOption(OPTION_SKIP_ROLE)) {
            roles.forEach(role -> added.add(new ClusterRolePrincipal(role)));
        }
        subject.getPrincipals().addAll(added);
        return true;
    }

    @Override
    public boolean abort() throws LoginException {
        return logout();
    }

    @Override
    public boolean logout() throws LoginException {
        subject.getPrincipals().removeAll(added);
        added.clear();
        roles.clear();
        loggedIn = false;
        return true;
    }

    /** The credentials the connecting side presented. */
    protected Credentials getCredentials() throws LoginException {
        return handle(new CredentialsCallback()).getCredentials();
    }

    /** The connecting side's address. */
    protected String getEndpoint() throws LoginException {
        return handle(new EndpointCallback()).getEndpoint();
    }

    /** The certificates the connecting side presented in the TLS handshake, if any. */
    protected List<Certificate> getCertificates() throws LoginException {
        Certificate[] certificates = handle(new CertificatesCallback()).getCertificates();
        return certificates == null ? Collections.emptyList() : List.of(certificates);
    }

    /** The cluster name the connecting side asked for. */
    protected String getClusterName() throws LoginException {
        return handle(new ClusterNameCallback()).getClusterName();
    }

    protected boolean getBoolOption(String name) {
        Object value = options == null ? null : options.get(name);
        return value != null && Boolean.parseBoolean(value.toString());
    }

    protected String getStringOption(String name, String defaultValue) {
        Object value = options == null ? null : options.get(name);
        return value == null ? defaultValue : value.toString();
    }

    private <C extends Callback> C handle(C callback) throws LoginException {
        try {
            callbackHandler.handle(new Callback[]{callback});
            return callback;
        } catch (IOException | UnsupportedCallbackException e) {
            LoginException exception = new LoginException("Callback " + callback.getClass().getSimpleName() + " failed");
            exception.initCause(e);
            throw exception;
        }
    }
}
