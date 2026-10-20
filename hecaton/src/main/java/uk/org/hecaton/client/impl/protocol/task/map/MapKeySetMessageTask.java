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

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.MapKeySetCodec;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.query.QueryResultRow;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.internal.util.IterationType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static uk.org.hecaton.map.impl.LocalMapStatsUtil.incrementOtherOperationsCount;

public class MapKeySetMessageTask
        extends DefaultMapQueryMessageTask<String> {

    public MapKeySetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object reduce(Collection<QueryResultRow> result) {
        List<Data> keys = new ArrayList<>(result.size());
        for (QueryResultRow resultEntry : result) {
            keys.add(resultEntry.getKey());
        }
        incrementOtherOperationsCount((MapService) getService(MapService.SERVICE_NAME), parameters);
        return keys;
    }

    @Override
    protected Predicate getPredicate() {
        return Predicates.alwaysTrue();
    }

    @Override
    protected IterationType getIterationType() {
        return IterationType.KEY;
    }

    @Override
    protected String decodeClientMessage(ClientMessage clientMessage) {
        return MapKeySetCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapKeySetCodec.encodeResponse((List<Data>) response);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.KEY_SET;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
