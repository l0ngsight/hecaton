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

package uk.org.hecaton.client.impl.protocol.codec.custom;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.Generated;
import uk.org.hecaton.client.impl.protocol.codec.builtin.*;

import static uk.org.hecaton.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static uk.org.hecaton.client.impl.protocol.ClientMessage.*;
import static uk.org.hecaton.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

/**
 * Wire layout of the AWS, GCP, Azure and Eureka discovery sections (formerly AwsConfigCodec,
 * GcpConfigCodec, AzureConfigCodec and EurekaConfigCodec, which shared it). Hecaton removed
 * these discovery mechanisms (D9) and keeps only the layout, for client compatibility.
 */
@SuppressWarnings("unused")
@Generated("e5191e444e0516d4b025e95df9e697e4")
public final class RemovedDiscoveryConfigCodec {
    private static final int ENABLED_FIELD_OFFSET = 0;
    private static final int USE_PUBLIC_IP_FIELD_OFFSET = ENABLED_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = USE_PUBLIC_IP_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;

    private RemovedDiscoveryConfigCodec() {
    }

    public static void encode(ClientMessage clientMessage, uk.org.hecaton.client.impl.protocol.codec.holder.RemovedDiscoveryConfigHolder config) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeBoolean(initialFrame.content, ENABLED_FIELD_OFFSET, config.isEnabled());
        encodeBoolean(initialFrame.content, USE_PUBLIC_IP_FIELD_OFFSET, config.isUsePublicIp());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, config.getTag());
        MapCodec.encode(clientMessage, config.getProperties(), StringCodec::encode, StringCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static uk.org.hecaton.client.impl.protocol.codec.holder.RemovedDiscoveryConfigHolder decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean enabled = decodeBoolean(initialFrame.content, ENABLED_FIELD_OFFSET);
        boolean usePublicIp = decodeBoolean(initialFrame.content, USE_PUBLIC_IP_FIELD_OFFSET);

        java.lang.String tag = StringCodec.decode(iterator);
        java.util.Map<java.lang.String, java.lang.String> properties = MapCodec.decode(iterator, StringCodec::decode, StringCodec::decode);

        fastForwardToEndFrame(iterator);

        return new uk.org.hecaton.client.impl.protocol.codec.holder.RemovedDiscoveryConfigHolder(tag, enabled, usePublicIp, properties);
    }
}
