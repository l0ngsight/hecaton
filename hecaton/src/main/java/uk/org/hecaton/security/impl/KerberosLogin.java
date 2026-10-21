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

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.util.HashMap;
import java.util.Map;

/**
 * Logs in to Kerberos with the JDK's {@code Krb5LoginModule}: with a keytab and principal when both
 * are given, otherwise from the ticket cache (F2, Kerberos authentication).
 */
public final class KerberosLogin {

    private static final String KRB5_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule";

    private KerberosLogin() {
    }

    /**
     * @param principal the Kerberos principal, or {@code null} to use the ticket cache's
     * @param keytab    the keytab file of the principal, or {@code null} to use the ticket cache
     * @param initiator {@code true} to get a ticket (a client), {@code false} only to hold the
     *                  service's keys (an acceptor)
     */
    public static Subject login(String principal, String keytab, boolean initiator) throws LoginException {
        Map<String, String> options = new HashMap<>();
        options.put("doNotPrompt", "true");
        options.put("refreshKrb5Config", "true");
        options.put("isInitiator", String.valueOf(initiator));
        if (keytab != null) {
            options.put("useKeyTab", "true");
            options.put("keyTab", keytab);
            options.put("storeKey", "true");
        } else {
            options.put("useTicketCache", "true");
        }
        if (principal != null) {
            options.put("principal", principal);
        }
        Configuration configuration = new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                return new AppConfigurationEntry[] {
                        new AppConfigurationEntry(KRB5_LOGIN_MODULE, LoginModuleControlFlag.REQUIRED, options)};
            }
        };
        LoginContext context = new LoginContext("hecaton-kerberos", new Subject(), null, configuration);
        context.login();
        return context.getSubject();
    }
}
