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

package uk.org.hecaton.jet.impl;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.DataPersistenceConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.JetException;
import uk.org.hecaton.jet.core.JetTestSupport;
import uk.org.hecaton.map.impl.proxy.MapProxyImpl;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.FileNotFoundException;

import static uk.org.hecaton.config.MapConfig.DEFAULT_BACKUP_COUNT;
import static uk.org.hecaton.jet.impl.JetServiceBackend.SQL_CATALOG_MAP_NAME;
import static uk.org.hecaton.jet.impl.JetServiceBackend.createSqlCatalogConfig;
import static org.junit.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JetServiceBackendTest extends JetTestSupport {
    @Test
    public void when_instanceIsCreated_then_sqlCatalogIsConfigured() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        HecatonInstance instance = createHecatonInstance(config);
        MapConfig mapConfig = instance.getConfig().getMapConfig(JetServiceBackend.SQL_CATALOG_MAP_NAME);
        assertEquals(createSqlCatalogConfig(), mapConfig);
    }

    @Test
    public void when_instanceIsCreatedWithOverriddenConfiguration_then_sqlCatalogConfigIsNotMerged() {
        Config config = new Config();
        DataPersistenceConfig dataPersistenceConfig = new DataPersistenceConfig();
        dataPersistenceConfig.setEnabled(true);
        config.addMapConfig(getMapConfig(SQL_CATALOG_MAP_NAME, dataPersistenceConfig));
        config.getJetConfig().setEnabled(true);

        HecatonInstance instance = createHecatonInstance(config);
        MapConfig mapConfig = instance.getConfig().getMapConfig(JetServiceBackend.SQL_CATALOG_MAP_NAME);
        assertEquals(new DataPersistenceConfig(), mapConfig.getDataPersistenceConfig());
        assertEquals(createSqlCatalogConfig(), mapConfig);

        MapConfig otherMapConfig = ((MapProxyImpl) instance.getMap("otherMap")).getMapConfig();
        assertFalse(otherMapConfig.getDataPersistenceConfig().isEnabled());
    }

    @Test
    public void when_instanceIsCreatedWithOverriddenDefaultConfiguration_then_sqlCatalogConfigIsNotMerged() {
        Config config = new Config();
        DataPersistenceConfig dataPersistenceConfig = new DataPersistenceConfig();
        dataPersistenceConfig.setEnabled(true);
        config.addMapConfig(getMapConfig("default", dataPersistenceConfig));
        config.getJetConfig().setEnabled(true);

        HecatonInstance instance = createHecatonInstance(config);
        MapConfig mapConfig = instance.getConfig().getMapConfig(JetServiceBackend.SQL_CATALOG_MAP_NAME);
        assertEquals(new DataPersistenceConfig(), mapConfig.getDataPersistenceConfig());
        assertEquals(createSqlCatalogConfig(), mapConfig);
    }

    @Test
    public void wrapFileNotFoundException() {
        String message = "file not found";
        FileNotFoundException fileNotFound = new FileNotFoundException(message);
        assertThatThrownBy(() -> JetServiceBackend.wrapWithJetException(fileNotFound))
                .isInstanceOf(JetException.class)
                .hasRootCauseInstanceOf(FileNotFoundException.class)
                .hasMessageContaining(message);
    }

    @Test
    public void wrapJetException() {
        String message = "jet exception";
        JetException jetException = new JetException(message);
        assertThatThrownBy(() -> JetServiceBackend.wrapWithJetException(jetException))
                .isInstanceOf(JetException.class)
                .hasNoCause()
                .hasMessageContaining(message);
    }

    @Test
    public void when_instanceIsCreatedWithOverriddenDefaultConfiguration_then_defaultConfigurationIsNotChanged() {
        Config config = new Config();
        DataPersistenceConfig dataPersistenceConfig = new DataPersistenceConfig();
        dataPersistenceConfig.setEnabled(true);
        config.addMapConfig(getMapConfig("default", dataPersistenceConfig));
        config.getJetConfig().setEnabled(true);

        HecatonInstance instance = createHecatonInstance(config);
        MapConfig otherMapConfig = ((MapProxyImpl) instance.getMap("otherMap")).getMapConfig();
        assertTrue(otherMapConfig.getDataPersistenceConfig().isEnabled());
        assertEquals(DEFAULT_BACKUP_COUNT, otherMapConfig.getBackupCount());
    }

    @Test
    public void when_instanceIsCreatedWithOverriddenDefaultWildcardConfiguration_then_sqlCatalogConfigIsNotMerged() {
        Config config = new Config();
        DataPersistenceConfig dataPersistenceConfig = new DataPersistenceConfig();
        dataPersistenceConfig.setEnabled(true);
        config.addMapConfig(getMapConfig("*", dataPersistenceConfig));
        config.getJetConfig().setEnabled(true);

        HecatonInstance instance = createHecatonInstance(config);
        MapConfig mapConfig = instance.getConfig().getMapConfig(JetServiceBackend.SQL_CATALOG_MAP_NAME);
        assertEquals(new DataPersistenceConfig(), mapConfig.getDataPersistenceConfig());
        assertEquals(createSqlCatalogConfig(), mapConfig);
    }

    @Test
    public void when_instanceIsCreatedWithOverriddenDefaultWildcardConfiguration_then_defaultConfigurationIsNotChanged() {
        Config config = new Config();
        DataPersistenceConfig dataPersistenceConfig = new DataPersistenceConfig();
        dataPersistenceConfig.setEnabled(true);
        config.addMapConfig(getMapConfig("*", dataPersistenceConfig));
        config.getJetConfig().setEnabled(true);

        HecatonInstance instance = createHecatonInstance(config);
        MapConfig otherMapConfig = ((MapProxyImpl) instance.getMap("otherMap")).getMapConfig();
        assertTrue(otherMapConfig.getDataPersistenceConfig().isEnabled());
        assertEquals(DEFAULT_BACKUP_COUNT, otherMapConfig.getBackupCount());
    }

    private static MapConfig getMapConfig(String mapName, DataPersistenceConfig dataPersistenceConfig) {
        return new MapConfig(mapName).setDataPersistenceConfig(dataPersistenceConfig);
    }
}
