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
package uk.org.hecaton.security.impl;

import uk.org.hecaton.config.PermissionConfig;
import uk.org.hecaton.config.PermissionPolicyConfig;
import uk.org.hecaton.config.SecurityConfig;
import uk.org.hecaton.config.SecurityInterceptorConfig;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.ClassLoaderUtil;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.server.tcp.TcpServerConnection;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.security.Credentials;
import uk.org.hecaton.security.ICredentialsFactory;
import uk.org.hecaton.security.IPermissionPolicy;
import uk.org.hecaton.security.Parameters;
import uk.org.hecaton.security.SecureCallable;
import uk.org.hecaton.security.SecurityInterceptor;
import uk.org.hecaton.security.UsernamePasswordCredentials;
import uk.org.hecaton.sql.impl.security.SqlSecurityContext;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.AccessControlException;
import java.security.Permission;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;

/**
 * Authentication (F2) and authorization (F3) when security is enabled: logs members and clients
 * in through the JAAS login modules of their realm, and checks client operations against the
 * permission policy.
 */
public class SecurityContextImpl implements InternalSecurityContext {

    private static final ThreadLocal<Subject> CLIENT_TASK_SUBJECT = new ThreadLocal<>();

    private final Node node;
    private final ILogger logger;
    private final SecurityConfig securityConfig;
    private final IPermissionPolicy permissionPolicy;
    private final List<SecurityInterceptor> interceptors;
    private final ICredentialsFactory credentialsFactory;
    private volatile Set<PermissionConfig> clientPermissions;

    public SecurityContextImpl(Node node) {
        this.node = node;
        this.logger = node.getLogger(SecurityContextImpl.class);
        this.securityConfig = node.getConfig().getSecurityConfig();
        this.clientPermissions = Set.copyOf(securityConfig.getClientPermissionConfigs());
        this.permissionPolicy = createPermissionPolicy();
        this.interceptors = createInterceptors();
        ICredentialsFactory factory = securityConfig.getRealmCredentialsFactory(securityConfig.getMemberRealm());
        // without an identity, the member presents empty credentials
        this.credentialsFactory = factory == null ? new NoCredentialsFactory() : factory;
        credentialsFactory.configure(new SecurityCallbackHandler(node, this, node.getConfig().getClusterName(), null, null,
                () -> null));
    }

    @Override
    public LoginContext createMemberLoginContext(String clusterName, Credentials credentials, Connection connection)
            throws LoginException {
        return login(securityConfig.getMemberRealm(), clusterName, credentials, connection);
    }

    @Override
    public LoginContext createClientLoginContext(String clusterName, Credentials credentials, Connection connection)
            throws LoginException {
        return login(securityConfig.getClientRealm(), clusterName, credentials, connection);
    }

    private LoginContext login(String realm, String clusterName, Credentials credentials, Connection connection)
            throws LoginException {
        InetSocketAddress remote = connection == null ? null : connection.getRemoteSocketAddress();
        SecurityCallbackHandler handler = new SecurityCallbackHandler(node, this, clusterName, credentials,
                remote == null ? null : remote.getAddress(), () -> peerCertificates(connection));
        return newLoginContext(createLoginConfigurationForRealm(realm), handler);
    }

    @Override
    public Configuration createLoginConfigurationForRealm(String realmName) {
        return new RealmLoginConfiguration(securityConfig.getRealmLoginModuleConfigs(realmName));
    }

    @Override
    public LoginContext createLoginContext(@Nonnull Configuration configuration, String clusterName, Credentials credentials,
                                           InetAddress remoteAddress) throws LoginException {
        return newLoginContext(configuration,
                new SecurityCallbackHandler(node, this, clusterName, credentials, remoteAddress, () -> null));
    }

    private LoginContext newLoginContext(Configuration configuration, SecurityCallbackHandler handler) throws LoginException {
        // JAAS loads the login modules through the context class loader
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(node.getConfigClassLoader());
        try {
            return new HecatonLoginContext(new Subject(), handler, configuration, node.getConfigClassLoader());
        } finally {
            thread.setContextClassLoader(previous);
        }
    }

    /** The certificates the peer of {@code connection} presented in the TLS handshake, if any. */
    static Certificate[] peerCertificates(Connection connection) {
        if (connection instanceof TcpServerConnection tcp && tcp.getChannel() != null) {
            Object session = tcp.getChannel().attributeMap().get(SSLSession.class);
            if (session instanceof SSLSession ssl) {
                try {
                    return ssl.getPeerCertificates();
                } catch (SSLPeerUnverifiedException e) {
                    return null;
                }
            }
        }
        return null;
    }

    /** This member's own credentials, from its member realm's identity, or {@code null} if it has none. */
    Credentials memberIdentity() {
        return credentialsFactory instanceof NoCredentialsFactory ? null : credentialsFactory.newCredentials();
    }

    @Override
    public ICredentialsFactory getCredentialsFactory() {
        return credentialsFactory;
    }

    @Override
    public void checkPermission(Subject subject, Permission permission) throws AccessControlException {
        if (subject == null || !permissionPolicy.getPermissions(subject, permission.getClass()).implies(permission)) {
            throw new AccessControlException("Permission " + permission + " denied", permission);
        }
    }

    @Override
    public void interceptBefore(Credentials credentials, String serviceName, String objectName, String methodName,
                                Object[] parameters) throws AccessControlException {
        Parameters params = new ParametersImpl(parameters);
        for (SecurityInterceptor interceptor : interceptors) {
            interceptor.before(credentials, serviceName, objectName, methodName, params);
        }
    }

    @Override
    public void interceptAfter(Credentials credentials, String serviceName, String objectName, String methodName) {
        Parameters params = new ParametersImpl(null);
        for (SecurityInterceptor interceptor : interceptors) {
            try {
                interceptor.after(credentials, serviceName, objectName, methodName, params);
            } catch (RuntimeException e) {
                logger.warning("Security interceptor " + interceptor + " failed", e);
            }
        }
    }

    @Override
    public <V> SecureCallable<V> createSecureCallable(Subject subject, Callable<V> callable) {
        return new SecureCallableImpl<>(subject, callable);
    }

    @Override
    public <V> SecureCallable<?> createSecureCallable(Subject subject, Runnable runnable) {
        return new SecureCallableImpl<>(subject, Executors.callable(runnable));
    }

    /** Runs a client's task, so that {@link #isThreadRunningClientTask()} is {@code true} meanwhile. */
    <V> V runAsClientTask(Subject subject, Callable<V> task) throws Exception {
        Subject previous = CLIENT_TASK_SUBJECT.get();
        CLIENT_TASK_SUBJECT.set(subject);
        try {
            return task.call();
        } finally {
            if (previous == null) {
                CLIENT_TASK_SUBJECT.remove();
            } else {
                CLIENT_TASK_SUBJECT.set(previous);
            }
        }
    }

    @Override
    public boolean isThreadRunningClientTask() {
        return CLIENT_TASK_SUBJECT.get() != null;
    }

    @Override
    public void refreshPermissions(Set<PermissionConfig> permissionConfigs) {
        permissionPolicy.refreshPermissions(permissionConfigs);
        clientPermissions = Set.copyOf(permissionConfigs);
    }

    /** The client permissions in force on this member. */
    public Set<PermissionConfig> getClientPermissions() {
        return clientPermissions;
    }

    @Override
    public SqlSecurityContext createSqlContext(Subject subject) {
        return new SqlSecurityContext() {
            @Override
            public boolean isSecurityEnabled() {
                return true;
            }

            @Override
            public void checkPermission(Permission permission) {
                SecurityContextImpl.this.checkPermission(subject, permission);
            }

            @Override
            public Subject subject() {
                return subject;
            }
        };
    }

    @Override
    public void destroy() {
        permissionPolicy.destroy();
        credentialsFactory.destroy();
    }

    private IPermissionPolicy createPermissionPolicy() {
        PermissionPolicyConfig config = securityConfig.getClientPolicyConfig();
        IPermissionPolicy policy = config.getImplementation();
        try {
            if (policy == null) {
                policy = config.getClassName() == null || config.getClassName().isBlank()
                        ? new DefaultPermissionPolicy()
                        : ClassLoaderUtil.newInstance(node.getConfigClassLoader(), config.getClassName());
            }
        } catch (Exception e) {
            throw rethrow(e);
        }
        policy.configure(node.getConfig(), config.getProperties());
        return policy;
    }

    private List<SecurityInterceptor> createInterceptors() {
        List<SecurityInterceptor> list = new ArrayList<>();
        for (SecurityInterceptorConfig config : securityConfig.getSecurityInterceptorConfigs()) {
            SecurityInterceptor interceptor = config.getImplementation();
            try {
                if (interceptor == null) {
                    interceptor = ClassLoaderUtil.newInstance(node.getConfigClassLoader(), config.getClassName());
                }
            } catch (Exception e) {
                throw rethrow(e);
            }
            list.add(interceptor);
        }
        return List.copyOf(list);
    }

    /**
     * The identity of a member realm without one: empty credentials, since joining requires
     * some (only the cluster name is checked then).
     */
    private static final class NoCredentialsFactory implements ICredentialsFactory {

        @Override
        public void configure(CallbackHandler callbackHandler) {
        }

        @Override
        public Credentials newCredentials() {
            return new UsernamePasswordCredentials(null, null);
        }

        @Override
        public void destroy() {
        }
    }

    /** The parameters of an intercepted call. */
    private record ParametersImpl(Object[] values) implements Parameters {

        @Override
        public int length() {
            return values == null ? 0 : values.length;
        }

        @Override
        public Object get(int index) {
            return values[index];
        }

        @Override
        public Iterator<Object> iterator() {
            return values == null ? List.of().iterator() : Arrays.asList(values).iterator();
        }
    }
}
