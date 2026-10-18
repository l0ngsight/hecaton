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
import uk.org.hecaton.client.impl.protocol.codec.MapValuesWithPredicateCodec;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.query.QueryResultRow;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.internal.util.IterationType;
import uk.org.hecaton.security.SecurityInterceptorConstants;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static uk.org.hecaton.map.impl.LocalMapStatsUtil.incrementOtherOperationsCount;

public class MapValuesWithPredicateMessageTask
        extends DefaultMapQueryMessageTask<MapValuesWithPredicateCodec.RequestParameters> {

    public MapValuesWithPredicateMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object reduce(Collection<QueryResultRow> result) {
        List<Data> values = new ArrayList<>(result.size());
        for (QueryResultRow resultEntry : result) {
            values.add(resultEntry.getValue());
        }
        incrementOtherOperationsCount((MapService) getService(MapService.SERVICE_NAME), parameters.name);
        return values;
    }

    @Override
    protected Predicate getPredicate() {
        return serializationService.toObject(parameters.predicate);
    }

    @Override
    protected IterationType getIterationType() {
        return IterationType.VALUE;
    }

    @Override
    protected MapValuesWithPredicateCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapValuesWithPredicateCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapValuesWithPredicateCodec.encodeResponse((List<Data>) response);
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.predicate};
    }


    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.VALUES;
    }
}
