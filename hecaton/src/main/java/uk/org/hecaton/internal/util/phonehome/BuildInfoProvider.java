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

package uk.org.hecaton.internal.util.phonehome;

import uk.org.hecaton.instance.BuildInfo;
import uk.org.hecaton.instance.impl.Node;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

import static uk.org.hecaton.internal.util.phonehome.PhoneHomeMetrics.BUILD_VERSION;
import static uk.org.hecaton.internal.util.phonehome.PhoneHomeMetrics.HECATON_DOWNLOAD_ID;
import static uk.org.hecaton.internal.util.phonehome.PhoneHomeMetrics.JAVA_CLASSPATH;
import static uk.org.hecaton.internal.util.phonehome.PhoneHomeMetrics.JAVA_VERSION_OF_SYSTEM;
import static java.lang.Math.min;

/**
 * Provides metadata about this instance
 */
class BuildInfoProvider implements MetricsProvider {
    static final String PARDOT_ID_ENV_VAR = "HC_PARDOT_ID";
    private static final int CLASSPATH_MAX_LENGTH = 100_000;

    static String formatClassPath(String classpath) {
        String[] classPathEntries = classpath.split(File.pathSeparator);
        String shortenedEntries = Arrays.stream(classPathEntries)
                .filter(cpEntry -> cpEntry.endsWith(".jar"))
                .map(cpEntry -> cpEntry.substring(cpEntry.lastIndexOf(File.separator) + 1))
                .collect(Collectors.joining(","));
        return shortenedEntries.substring(0, min(CLASSPATH_MAX_LENGTH, shortenedEntries.length()));
    }

    @Override
    public void provideMetrics(Node node, MetricsCollectionContext context) {
        BuildInfo imdgInfo = node.getBuildInfo();
        context.collect(HECATON_DOWNLOAD_ID, getDownloadId());
        context.collect(JAVA_VERSION_OF_SYSTEM, System.getProperty("java.version"));
        context.collect(BUILD_VERSION, imdgInfo.getVersion());
        String classpath = System.getProperty("java.class.path");
        if (classpath != null) {
            context.collect(JAVA_CLASSPATH, formatClassPath(classpath));
        }
    }

    /**
     * Attempts to return the download ID for this instance or returns
     * {@code source} if unable to find the download ID.
     */
    private String getDownloadId() {
        String pardotId = System.getenv(PARDOT_ID_ENV_VAR);
        if (pardotId != null) {
            return pardotId;
        }
        try (InputStream is = getClass().getClassLoader()
                                        .getResourceAsStream("hecaton-download.properties")) {
            if (is != null) {
                Properties properties = new Properties();
                properties.load(is);
                return properties.getProperty("hecatonDownloadId");
            }
        } catch (IOException ignored) { }
        return "source";
    }
}
