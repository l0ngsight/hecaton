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

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.clientside.ClientConnectionManagerFactory;
import uk.org.hecaton.client.impl.connection.AddressProvider;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.core.HecatonInstance;

public class HecatonClientUtil {

    public static HecatonInstance newHecatonClient(ClientConfig config, AddressProvider addressProvider) {
        return HecatonClient.newHecatonClientInternal(config, null, null, addressProvider);
    }

    public static HecatonInstance newHecatonClient(
            ClientConfig config,
            ClientConnectionManagerFactory connectionManagerFactory,
            AddressProvider addressProvider
    ) {
        return HecatonClient.newHecatonClientInternal(config, null, connectionManagerFactory, addressProvider);
    }

    public static ClientMessage.Frame fastForwardToEndFrame(ClientMessage.ForwardFrameIterator iterator) {
        int numberOfExpectedEndFrames = 1;
        ClientMessage.Frame frame = null;
        while (numberOfExpectedEndFrames != 0) {
            frame = iterator.next();
            if (frame.isEndFrame()) {
                numberOfExpectedEndFrames--;
            } else if (frame.isBeginFrame()) {
                numberOfExpectedEndFrames++;
            }
        }
        return frame;
    }
}
