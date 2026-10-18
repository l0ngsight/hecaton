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

package uk.org.hecaton.map.impl.nearcache.invalidation;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExecuteOnKeyInvalidationTest extends HecatonTestSupport {

  private final TestHecatonFactory factory = new TestHecatonFactory();
  private HecatonInstance client;
  private HecatonInstance serializedKeyClient;

  @Before
  public void setUp() {
    startHecatonInstance();
    client = clientWithNearCache();
    serializedKeyClient = clientWithSerializedKeysNearCache();
  }

  @After
  public void tearDown() {
    factory.shutdownAll();
  }

  private HecatonInstance clientWithNearCache() {
    ClientConfig clientConfig = new ClientConfig().addNearCacheConfig(new NearCacheConfig("map"));
    return factory.newHecatonClient(clientConfig);
  }

  private HecatonInstance clientWithSerializedKeysNearCache() {
    ClientConfig clientConfig = new ClientConfig().addNearCacheConfig(
        new NearCacheConfig("serializedKeyMap").setSerializeKeys(true));
    return factory.newHecatonClient(clientConfig);
  }

  private void startHecatonInstance() {
    factory.newHecatonInstance();
  }

  @Test
  public void testExecuteOnKeysInvalidation_withSerializedKeys() {
    IMap<String, String> serializedKeyMap = serializedKeyClient.getMap("serializedKeyMap");
    serializedKeyMap.put("key", "old-value");
    serializedKeyMap.get("key");
    serializedKeyMap.executeOnKeys(
        Collections.singleton("key"),
        entry -> {
          entry.setValue("new-value");
          return null;
        }
    );
    assertEquals("new-value", serializedKeyMap.get("key"));
  }

  @Test
  public void testExecuteOnKeysInvalidation_withNullOperationResult() {
    IMap<String, String> map = client.getMap("map");
    map.put("key", "old-value");
    map.get("key");
    map.executeOnKeys(
        Collections.singleton("key"),
        entry -> {
          entry.setValue("new-value");
          return null;
        }
    );
    assertEquals("new-value", map.get("key"));
  }

  @Test
  public void testExecuteOnKeysInvalidation_withOperationResult() {
    IMap<String, String> map = client.getMap("map");
    map.put("key", "old-value");
    map.get("key");
    map.executeOnKeys(
        Collections.singleton("key"),
        entry -> {
          entry.setValue("new-value");
          return "computation-result-of-on-key-execution";
        }
    );
    assertEquals("new-value", map.get("key"));
  }
}
