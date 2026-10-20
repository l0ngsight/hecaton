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

package uk.org.hecaton.jet.impl.client.protocol.task;

import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.MemberLeftException;
import uk.org.hecaton.instance.SimpleMemberImpl;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.jet.core.JetTestSupport;
import uk.org.hecaton.jet.core.TestUtil;
import uk.org.hecaton.spi.exception.TargetNotMemberException;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.Accessors;
import uk.org.hecaton.test.mocknetwork.MockServerConnection;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class AbstractJetMultiTargetMessageTaskTest extends JetTestSupport {
    protected static final String OTHER_EXCEPTION_MESSAGE = "null";
    protected static final Map<Member, Object> IGNORED_EXCEPTIONS_RESULT =
            TestUtil.createMap(
                    new SimpleMemberImpl(), new MemberLeftException(),
                    new SimpleMemberImpl(), new TargetNotMemberException("")
            );
    protected static final Map<Member, Object> OTHER_EXCEPTION_RESULT =
            Collections.singletonMap(new SimpleMemberImpl(), new OtherException(OTHER_EXCEPTION_MESSAGE));

    protected Node node;
    protected Connection connection;

    @Before
    public void init() {
        HecatonInstance hecatonInstance = createHecatonInstance();
        NodeEngineImpl nodeEngine = Accessors.getNodeEngineImpl(hecatonInstance);
        node = nodeEngine.getNode();
        connection = new MockServerConnection(node.address, node.address, null, null, null, null);
    }

    protected static class OtherException extends Exception {
        public OtherException(String message) {
            super(message);
        }
    }

    protected Map<Member, Object> prepareSingleResultMap(Object result) {
        List<?> summaryList = Collections.singletonList(result);
        return Collections.singletonMap(new SimpleMemberImpl(), summaryList);
    }

    protected Map<Member, Object> prepareDuplicatedResult(Object result) {
        List<?> summaryList = Collections.singletonList(result);
        return TestUtil.createMap(
                new SimpleMemberImpl(), summaryList,
                new SimpleMemberImpl(), summaryList
        );
    }
}
