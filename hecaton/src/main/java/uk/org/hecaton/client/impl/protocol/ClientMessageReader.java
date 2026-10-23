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

package uk.org.hecaton.client.impl.protocol;

import uk.org.hecaton.client.impl.protocol.exception.MaxMessageSizeExceeded;
import uk.org.hecaton.internal.nio.Bits;

import static java.lang.String.format;

import java.nio.ByteBuffer;

import static uk.org.hecaton.client.impl.protocol.ClientMessage.IS_FINAL_FLAG;
import static uk.org.hecaton.client.impl.protocol.ClientMessage.SIZE_OF_FRAME_LENGTH_AND_FLAGS;

public final class ClientMessageReader {

    private static final int INT_MASK = 0xffff;
    private int readOffset = -1;
    private ClientMessage clientMessage;
    private int sumUntrustedMessageLength;
    private int sumTrustedMessageLength;
    private final int maxMessageLength;
    private final int maxTrustedMessageLength;

    public ClientMessageReader(int maxMessageLength) {
        this(maxMessageLength, Integer.MAX_VALUE);
    }

    /**
     * @param maxMessageLength        the largest message from a client that is not authenticated
     * @param maxTrustedMessageLength the largest message from an authenticated client (load protection, F23)
     */
    public ClientMessageReader(int maxMessageLength, int maxTrustedMessageLength) {
        this.maxMessageLength = maxMessageLength > 0 ? maxMessageLength : Integer.MAX_VALUE;
        this.maxTrustedMessageLength = maxTrustedMessageLength > 0 ? maxTrustedMessageLength : Integer.MAX_VALUE;
    }

    public boolean readFrom(ByteBuffer src, boolean trusted) {
        for (; ; ) {
            if (readFrame(src, trusted)) {
                if (ClientMessage.isFlagSet(clientMessage.getEndFrame().flags, IS_FINAL_FLAG)) {
                    return true;
                }
                readOffset = -1;
            } else {
                return false;
            }

        }
    }

    public ClientMessage getClientMessage() {
        return clientMessage;
    }

    public void reset() {
        readOffset = -1;
        clientMessage = null;
    }

    private boolean readFrame(ByteBuffer src, boolean trusted) {
        if (readOffset == -1) {
            // Check for the minimum buffer size only if we
            // haven't read the frame length and flags.
            int remaining = src.remaining();
            if (remaining < SIZE_OF_FRAME_LENGTH_AND_FLAGS) {
                // we don't have even the frame length and flags ready
                return false;
            }

            int frameLength = Bits.readIntL(src, src.position());
            if (frameLength < SIZE_OF_FRAME_LENGTH_AND_FLAGS) {
                throw new IllegalArgumentException(format(
                        "The client message frame reported illegal length (%d bytes)."
                                + " Minimal length is the size of frame header (%d bytes).",
                        frameLength, SIZE_OF_FRAME_LENGTH_AND_FLAGS));
            }
            if (!trusted) {
                // check the message size overflow and message size limit
                if (Integer.MAX_VALUE - frameLength < sumUntrustedMessageLength
                        || sumUntrustedMessageLength + frameLength > maxMessageLength) {
                    throw new MaxMessageSizeExceeded(
                            format("The client message size (%d + %d) exceeded the maximum allowed length (%d)",
                                    sumUntrustedMessageLength, frameLength, maxMessageLength));
                }
                sumUntrustedMessageLength += frameLength;
            } else if (maxTrustedMessageLength != Integer.MAX_VALUE) {
                // checked from the frame header, before the frame is allocated
                if (Integer.MAX_VALUE - frameLength < sumTrustedMessageLength
                        || sumTrustedMessageLength + frameLength > maxTrustedMessageLength) {
                    throw new MaxMessageSizeExceeded(
                            format("The client message size (%d + %d) exceeded the maximum allowed length (%d)",
                                    sumTrustedMessageLength, frameLength, maxTrustedMessageLength));
                }
                sumTrustedMessageLength += frameLength;
            }

            src.position(src.position() + Bits.INT_SIZE_IN_BYTES);
            int flags = Bits.readShortL(src, src.position()) & INT_MASK;
            src.position(src.position() + Bits.SHORT_SIZE_IN_BYTES);

            int size = frameLength - SIZE_OF_FRAME_LENGTH_AND_FLAGS;
            byte[] bytes = new byte[size];
            ClientMessage.Frame frame = new ClientMessage.Frame(bytes, flags);
            if (clientMessage == null) {
                clientMessage = ClientMessage.createForDecode(frame);
            } else {
                clientMessage.add(frame);
            }
            readOffset = 0;
            if (size == 0) {
                return true;
            }
        }

        ClientMessage.Frame frame = clientMessage.getEndFrame();
        return accumulate(src, frame.content, frame.content.length - readOffset);
    }

    private boolean accumulate(ByteBuffer src, byte[] dest, int length) {
        int remaining = src.remaining();
        int readLength = remaining < length ? remaining : length;
        if (readLength > 0) {
            src.get(dest, readOffset, readLength);
            readOffset += readLength;
            return readLength == length;
        }
        return false;
    }

}
