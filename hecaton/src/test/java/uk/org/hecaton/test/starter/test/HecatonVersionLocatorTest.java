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

package uk.org.hecaton.test.starter.test;

import static uk.org.hecaton.test.starter.HecatonVersionLocator.Artifact.EE_JAR;
import static uk.org.hecaton.test.starter.HecatonVersionLocator.Artifact.OS_JAR;
import static uk.org.hecaton.test.starter.HecatonVersionLocator.Artifact.OS_TEST_JAR;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import uk.org.hecaton.test.annotation.NightlyTest;
import uk.org.hecaton.test.starter.HecatonVersionLocator;
import uk.org.hecaton.test.starter.HecatonVersionLocator.Artifact;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * TODO This test doesn't force a re-download, so if an artifact is cached in the local repository, the download won't be
 *      exercised. It's difficult to modify the local Maven repository as it's not encapsulated for the scope of testing
 */
@NightlyTest
class HecatonVersionLocatorTest {
    private static HashFunction hashFunction;

    @BeforeAll
    static void setUp() {
        hashFunction = Hashing.crc32c();
    }

    static Stream<Arguments> testDownloadVersion() {
        return Stream.of(Arguments.of(OS_JAR, "4db18099"), Arguments.of(OS_TEST_JAR, "80f97565"),
                Arguments.of(EE_JAR, "806220c1"));
    }

    @ParameterizedTest
    @MethodSource("testDownloadVersion")
    void testDownloadVersion(Artifact artifact, String expectedHash) throws IOException {
        Map<Artifact, File> files = HecatonVersionLocator.locateVersion("4.0", artifact.isEnterprise());
        final File file = files.get(artifact);
        Objects.requireNonNull(file, MessageFormat.format("Failed to find {0} JAR in {1}", artifact, files));
        final HashCode memberHash = Files.asByteSource(file).hash(hashFunction);
        assertEquals(expectedHash, memberHash.toString(),
                MessageFormat.format("Unexpected hash of Hecaton {0} JAR", artifact));
    }
}
