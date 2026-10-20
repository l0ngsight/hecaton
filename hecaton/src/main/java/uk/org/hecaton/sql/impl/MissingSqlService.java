/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.sql.impl;

import uk.org.hecaton.internal.serialization.ReflectionClassNameFilter;
import uk.org.hecaton.jet.impl.exception.JetDisabledException;
import uk.org.hecaton.jet.impl.util.Util;
import uk.org.hecaton.sql.HecatonSqlException;
import uk.org.hecaton.sql.SqlResult;
import uk.org.hecaton.sql.SqlStatement;
import uk.org.hecaton.sql.impl.security.SqlSecurityContext;
import uk.org.hecaton.sql.impl.state.QueryClientStateRegistry;

import javax.annotation.Nonnull;
import java.util.UUID;

public class MissingSqlService implements InternalSqlService {

    private final UUID localMemberId;
    private final boolean isJetEnabled;

    public MissingSqlService(UUID localMemberId, boolean isJetEnabled) {
        this.localMemberId = localMemberId;
        this.isJetEnabled = isJetEnabled;
    }

    @Nonnull
    @Override
    public SqlResult execute(@Nonnull SqlStatement statement) {
        throw throwDisabled();
    }

    @Override
    public void start() {
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void closeOnError(QueryId queryId) {
        // We still want to send an exception to the client, so, just leave method impl empty.
    }

    @Override
    public SqlResult execute(@Nonnull SqlStatement statement, SqlSecurityContext securityContext) {
        throw throwDisabled();
    }

    @Override
    public SqlResult execute(@Nonnull SqlStatement statement, SqlSecurityContext securityContext, QueryId queryId) {
        throw throwDisabled();
    }

    @Override
    public SqlResult execute(@Nonnull SqlStatement statement, SqlSecurityContext securityContext, QueryId queryId,
                             boolean skipStats) {
        throw throwDisabled();
    }

    @Override
    public QueryClientStateRegistry getClientStateRegistry() {
        throw throwDisabled();
    }

    @Override
    public long getSqlQueriesSubmittedCount() {
        return 0;
    }

    @Override
    public long getSqlStreamingQueriesExecutedCount() {
        return 0;
    }

    @Override
    public String mappingDdl(String name) {
        throw throwDisabled();
    }

    @Override
    public ReflectionClassNameFilter getReflectionClassNameFilter() {
        throw throwDisabled();
    }

    private RuntimeException throwDisabled() {
        if (!isJetEnabled) {
            throw new JetDisabledException(Util.JET_IS_DISABLED_MESSAGE);
        }
        throw new HecatonSqlException(
                localMemberId,
                SqlErrorCode.GENERIC,
                "Cannot execute SQL query because \"hecaton-sql\" module is not on the classpath",
                null,
                null);
    }

    @Override
    public Object getOptimizer() {
        throw throwDisabled();
    }
}
