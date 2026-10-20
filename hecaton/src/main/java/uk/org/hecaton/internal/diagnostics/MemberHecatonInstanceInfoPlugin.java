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

package uk.org.hecaton.internal.diagnostics;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.instance.impl.NodeState;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.properties.HecatonProperty;

import java.util.UUID;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Prints all kinds of Hecaton member specific info.
 * <p>
 * Lots of other information is already captured through the metrics.
 */
public class MemberHecatonInstanceInfoPlugin extends DiagnosticsPlugin {

    /**
     * The period in seconds the HecatonMemberInstanceInfoPlugin runs.
     * <p>
     * This plugin is very cheap to use.
     * <p>
     * If set to 0, the plugin is disabled.
     */
    public static final HecatonProperty PERIOD_SECONDS = new HecatonProperty(
            "hecaton.diagnostics.memberinfo.period.seconds", 60, SECONDS);

    private long periodMillis;
    private final NodeEngineImpl nodeEngine;


    public MemberHecatonInstanceInfoPlugin(NodeEngineImpl nodeEngine) {
        super(nodeEngine.getLogger(MemberHecatonInstanceInfoPlugin.class));
        this.nodeEngine = nodeEngine;
        readProperties();
    }

    @Override
    public long getPeriodMillis() {
        return periodMillis;
    }

    @Override
    public void onStart() {
        super.onStart();
        logger.info("Plugin:active, period-millis:" + periodMillis);
    }

    @Override
    public void onShutdown() {
        super.onShutdown();
        logger.info("Plugin:inactive");
    }

    @Override
    void readProperties() {
        this.periodMillis = nodeEngine.getProperties().getMillis(overrideProperty(PERIOD_SECONDS));
    }

    @Override
    public void run(DiagnosticsLogWriter writer) {
        if (!isActive()) {
            return;
        }
        writer.startSection("HecatonInstance");

        writer.writeKeyValueEntry("thisAddress", nodeEngine.getNode().getThisAddress().toString());
        writer.writeKeyValueEntry("isRunning", nodeEngine.getNode().isRunning());
        writer.writeKeyValueEntry("isLite", nodeEngine.getNode().isLiteMember());
        writer.writeKeyValueEntry("joined", nodeEngine.getNode().getClusterService().isJoined());
        NodeState state = nodeEngine.getNode().getState();
        writer.writeKeyValueEntry("nodeState", state == null ? "null" : state.toString());

        UUID clusterId = nodeEngine.getClusterService().getClusterId();
        writer.writeKeyValueEntry("clusterId", clusterId != null ? clusterId.toString() : "null");
        writer.writeKeyValueEntry("clusterSize", nodeEngine.getClusterService().getSize());
        writer.writeKeyValueEntry("isMaster", nodeEngine.getClusterService().isMaster());
        Address masterAddress = nodeEngine.getClusterService().getMasterAddress();
        writer.writeKeyValueEntry("masterAddress", masterAddress == null ? "null" : masterAddress.toString());

        writer.startSection("Members");
        for (Member member : nodeEngine.getClusterService().getMemberImpls()) {
            writer.writeEntry(member.getAddress().toString());
        }
        writer.endSection();

        writer.endSection();
    }
}
