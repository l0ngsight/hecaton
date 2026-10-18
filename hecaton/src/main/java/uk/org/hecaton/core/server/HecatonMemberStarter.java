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

package uk.org.hecaton.core.server;

import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

/**
 * Starts a Hecaton Member.
 */
public final class HecatonMemberStarter {

    private HecatonMemberStarter() {
    }

    /**
     * Creates a server instance of Hecaton.
     * <p>
     * If user sets the system property "print.port", the server writes the port number of the Hecaton instance to a file.
     * The file name is the same as the "print.port" property.
     *
     * @param args none
     */
    public static void main(String[] args) throws IOException {
        System.setProperty("hecaton.tracking.server", "true");
        HecatonInstance hz = Hecaton.newHecatonInstance();
        printMemberPort(hz);
    }

    private static void printMemberPort(HecatonInstance hz) throws IOException {
        String printPort = System.getProperty("print.port");
        if (printPort != null) {
            try (PrintWriter printWriter = new PrintWriter("ports" + File.separator + printPort, StandardCharsets.UTF_8)) {
                printWriter.println(hz.getCluster().getLocalMember().getAddress().getPort());
            }
        }
    }
}
