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

package uk.org.hecaton.client;

import uk.org.hecaton.client.impl.connection.tcp.TcpClientConnectionManager;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.DefaultOutOfMemoryHandler;

import static uk.org.hecaton.internal.util.EmptyStatement.ignore;

/**
 * To clear resources of the client upon OutOfMemory
 */
public class ClientOutOfMemoryHandler extends DefaultOutOfMemoryHandler {

    @Override
    public void onOutOfMemory(OutOfMemoryError oome, HecatonInstance[] hecatonInstances) {
        for (HecatonInstance instance : hecatonInstances) {
            if (instance instanceof HecatonClientInstanceImpl impl) {
                ClientHelper.cleanResources(impl);
            }
        }
        try {
            oome.printStackTrace(System.err);
        } catch (Throwable ignored) {
            ignore(ignored);
        }
    }

    private static final class ClientHelper {

        private ClientHelper() {
        }

        static void cleanResources(HecatonClientInstanceImpl client) {
            closeSockets(client);
            tryShutdown(client);
        }

        private static void closeSockets(HecatonClientInstanceImpl client) {
            TcpClientConnectionManager connectionManager = (TcpClientConnectionManager) client.getConnectionManager();
            if (connectionManager != null) {
                try {
                    connectionManager.shutdown();
                } catch (Throwable ignored) {
                    ignore(ignored);
                }
            }
        }

        private static void tryShutdown(HecatonClientInstanceImpl client) {
            try {
                client.doShutdown();
            } catch (Throwable ignored) {
                ignore(ignored);
            }
        }
    }
}
