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

package uk.org.hecaton.jet.impl.util;

import uk.org.hecaton.jet.core.JetTestSupport;
import uk.org.hecaton.jet.core.test.JetAssert;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static uk.org.hecaton.jet.impl.util.IOUtil.packDirectoryIntoZip;
import static uk.org.hecaton.jet.impl.util.IOUtil.unzip;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IOUtilTest extends JetTestSupport {

    @Rule
    public TestName testName = new TestName();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void test_zipAndUnzipNestedFolder_then_contentsShouldBeSame() throws Exception {
        Path originalPath = Paths.get(this.getClass().getResource("/nested").toURI());
        test(originalPath);
    }

    @Test
    public void test_zipAndUnzipNestedFoldersWithAnEmptySubFolder_then_contentsShouldBeSame() throws Exception {
        Path originalPath = Paths.get(this.getClass().getResource("/nested").toURI());
        Path randomFile = originalPath.resolve(randomName());
        Files.createDirectory(randomFile);
        try {
            test(originalPath);
        } finally {
            uk.org.hecaton.internal.nio.IOUtil.delete(randomFile);
        }
    }

    public void test(Path originalPath) throws IOException {
        //Given
        ByteArrayOutputStream baos = new ByteArrayOutputStream(1000);

        //When
        packDirectoryIntoZip(originalPath, baos);
        Path unzippedPath = temporaryFolder.newFolder().toPath();
        unzip(new ByteArrayInputStream(baos.toByteArray()), unzippedPath);

        //Then
        Set<Path> originalSet = getChildrenFromRoot(originalPath);
        Set<Path> unzippedSet = getChildrenFromRoot(unzippedPath);

        assertCollection(originalSet, unzippedSet);

        try (Stream<Path> walk = Files.walk(unzippedPath)) {
            boolean allMatch = walk
                    .filter(Files::isRegularFile)
                    .allMatch(p -> {
                        try (Stream<String> lines = Files.lines(p)) {
                            String line = lines.iterator().next();
                            return line.equals(p.getFileName().toString() + " content");
                        } catch (IOException e) {
                            throw ExceptionUtil.rethrow(e);
                        }
                    });
            JetAssert.assertTrue("File contents are not matching", allMatch);
        }
    }

    private Set<Path> getChildrenFromRoot(Path path) throws IOException {
        try (Stream<Path> walk = Files.walk(path)) {
            return walk
                    .map(path::relativize)
                    .filter(p -> !p.toString().isEmpty())
                    .collect(Collectors.toSet());
        }
    }

    @Test
    public void when_zipSlipVulnerability_then_zipEntryIgnored_absolutePath() throws Exception {
        when_zipSlipVulnerability_then_zipEntryIgnored(
                (targetDir, nonTargetDir) -> nonTargetDir.resolve("file.txt").resolve("file.txt"));
    }

    @Test
    public void when_zipSlipVulnerability_then_zipEntryIgnored_relativePath() throws Exception {
        when_zipSlipVulnerability_then_zipEntryIgnored(
                (targetDir, nonTargetDir) -> targetDir.relativize(nonTargetDir).resolve("file.txt"));
    }

    @Test
    public void when_zipSlipVulnerability_then_zipEntryIgnored_goingUpALot() throws Exception {
        when_zipSlipVulnerability_then_zipEntryIgnored(
                (targetDir, nonTargetDir) -> Paths.get("..", "..", "..", "..", "..", "..", "..", "..", "file.txt"));
    }

    /**
     * @param entryFunction maps from (targetDir, nonTargetDir) to an entry path
     */
    private void when_zipSlipVulnerability_then_zipEntryIgnored(BiFunction<Path, Path, Path> entryFunction)
            throws Exception {
        Path tmpTargetDir = Files.createTempDirectory(testName.getMethodName());
        Path tmpNonTargetDir = Files.createTempDirectory(testName.getMethodName());
        try {
            byte[] zipFile;
            String entryName = entryFunction.apply(tmpTargetDir, tmpNonTargetDir).toString();
            try (
                    ByteArrayOutputStream baos = new ByteArrayOutputStream(32 * 1024);
                    ZipOutputStream zos = new ZipOutputStream(baos)
            ) {
                zos.putNextEntry(new ZipEntry(entryName));
                zos.write("foo".getBytes());
                zos.closeEntry();
                zos.close();
                zipFile = baos.toByteArray();
            }

            assertThatThrownBy(() -> unzip(new ByteArrayInputStream(zipFile), tmpTargetDir))
                    .hasMessage("Entry with an illegal path: " + entryName);
            long count;
            try (Stream<Path> list = Files.list(tmpNonTargetDir)) {
                count = list.count();
            }
            assertEquals(0, count);
        } finally {
            uk.org.hecaton.internal.nio.IOUtil.delete(tmpTargetDir);
            uk.org.hecaton.internal.nio.IOUtil.delete(tmpNonTargetDir);
        }
    }
}
