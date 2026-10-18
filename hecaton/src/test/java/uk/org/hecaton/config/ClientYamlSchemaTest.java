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

package uk.org.hecaton.config;

import org.json.JSONObject;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class ClientYamlSchemaTest
        extends AbstractYamlSchemaTest {

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> buildTestcases() {
        return Stream.concat(
                buildTestcases("uk/org/hecaton/config/yaml/testcases/client/").stream(),
                buildTestcases("uk/org/hecaton/config/yaml/testcases/common/").stream()
                        .map(ClientYamlSchemaTest::changeInputRootElem)
        ).collect(toList());
    }

    private static Object[] changeInputRootElem(Object[] args) {
        JSONObject input = (JSONObject) args[1];
        Object hecatonValue = input.getJSONObject("hecaton");
        input.remove("hecaton");
        input.put("hecaton-client", hecatonValue);
        amendInstancePointersForClient((JSONObject) args[2]);
        return new Object[]{args[0], input, args[2]};
    }

    private static void amendInstancePointersForClient(JSONObject error) {
        if (error == null) {
            return;
        }
        String pointerToViolation = error.optString("pointerToViolation");
        if (pointerToViolation != null) {
            error.put("pointerToViolation", pointerToViolation.replace("#/hecaton", "#/hecaton-client"));
            error.optJSONArray("causingExceptions")
                    .forEach(causingExcJson -> amendInstancePointersForClient((JSONObject) causingExcJson));
        }
    }
}
