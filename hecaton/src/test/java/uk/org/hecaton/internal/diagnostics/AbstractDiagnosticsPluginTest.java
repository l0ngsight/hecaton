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

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonTestSupport;
import org.junit.Before;

import java.io.CharArrayWriter;
import java.io.File;
import java.io.PrintWriter;
import java.lang.reflect.Field;

import static uk.org.hecaton.internal.nio.IOUtil.deleteQuietly;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;

public class AbstractDiagnosticsPluginTest extends HecatonTestSupport {

    protected DiagnosticsLogWriterImpl logWriter;
    private CharArrayWriter out;

    @Before
    public final void setupLogWriter() {
        logWriter = new DiagnosticsLogWriterImpl();
        out = new CharArrayWriter();
        logWriter.init(new PrintWriter(out));
    }

    protected void reset() {
        out.reset();
    }

    protected String getContent() {
        return out.toString();
    }

    protected void assertContains(String expected) {
        assertContains(getContent(), expected);
    }

    protected void assertNotContains(String expected) {
        assertNotContains(getContent(), expected);
    }

    protected static Diagnostics getDiagnostics(HecatonInstance hecatonInstance) {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(hecatonInstance);
        try {
            Field field = NodeEngineImpl.class.getDeclaredField("diagnostics");
            field.setAccessible(true);
            return (Diagnostics) field.get(nodeEngine);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static void cleanupDiagnosticFiles(Diagnostics diagnostics) {
        if (diagnostics == null) {
            return;
        }
        File[] files = diagnostics.getLoggingDirectory().listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            String name = file.getName();
            if (name.startsWith(diagnostics.getBaseFileNameWithTime()) && name.endsWith(".log")) {
                deleteQuietly(file);
            }
        }
    }
}
