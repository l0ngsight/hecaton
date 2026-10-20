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

package uk.org.hecaton.client.impl.protocol.task.map;

import uk.org.hecaton.aggregation.Aggregator;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.map.impl.query.QueryResult;
import uk.org.hecaton.map.impl.query.QueryResultUtils;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.MapPermission;
import uk.org.hecaton.internal.util.IterationType;
import uk.org.hecaton.security.permission.UserCodeNamespacePermission;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public abstract class DefaultMapProjectMessageTask<P>
        extends AbstractMapQueryMessageTask<P,
        QueryResult, QueryResult, List<Data>> {

    public DefaultMapProjectMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected IterationType getIterationType() {
        return IterationType.VALUE;
    }

    @Override
    protected Aggregator<?, ?> getAggregator() {
        return null;
    }

    @Override
    protected Predicate getPredicate() {
        return Predicates.alwaysTrue();
    }

    @Override
    protected void extractAndAppendResult(Collection<QueryResult> results, QueryResult result) {
        results.add(result);
    }

    @Override
    protected List<Data> reduce(Collection<QueryResult> results) {
        if (results.isEmpty()) {
            return Collections.emptyList();
        }

        QueryResult combinedResult = null;
        for (QueryResult result : results) {
            if (combinedResult == null) {
                combinedResult = result;
            } else {
                combinedResult.combine(result);
            }
        }

        Set result = QueryResultUtils.transformToSet(nodeEngine.getSerializationService(), combinedResult,
                getPredicate(), IterationType.VALUE, false, true);

        List<Data> serialized = new ArrayList<>(result.size());

        for (Object row : result) {
            serialized.add((Data) row);
        }

        return serialized;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(getDistributedObjectName(), ActionConstants.ACTION_PROJECTION);
    }

    @Override
    public Permission getUserCodeNamespacePermission() {
        String namespace = getUserCodeNamespace();
        return namespace != null ? new UserCodeNamespacePermission(namespace, ActionConstants.ACTION_USE) : null;
    }
}
