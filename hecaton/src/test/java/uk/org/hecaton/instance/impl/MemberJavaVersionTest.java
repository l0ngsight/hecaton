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

package uk.org.hecaton.instance.impl;

import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemberJavaVersionTest {

    @Test
    public void java25AndLaterRunMembers() {
        assertThatCode(() -> MemberJavaVersion.check(25)).doesNotThrowAnyException();
        assertThatCode(() -> MemberJavaVersion.check(26)).doesNotThrowAnyException();
        assertThatCode(MemberJavaVersion::check).doesNotThrowAnyException();
    }

    @Test
    public void olderJavaDoesNotRunMembers() {
        assertThatThrownBy(() -> MemberJavaVersion.check(21))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Hecaton members require Java 25+; the client library supports Java 17+ (this is Java 21)");
    }
}
