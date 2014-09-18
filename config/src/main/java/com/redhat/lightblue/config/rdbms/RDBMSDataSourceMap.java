/*
 Copyright 2013 Red Hat, Inc. and/or its affiliates.

 This file is part of lightblue.

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.redhat.lightblue.config.rdbms;

import com.redhat.lightblue.common.rdbms.RDBMSDataSourceResolver;
import com.redhat.lightblue.common.rdbms.RDBMSDataStore;
import com.redhat.lightblue.config.DataSourceConfiguration;
import com.redhat.lightblue.config.DataSourcesConfiguration;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author lcestari
 */
public class RDBMSDataSourceMap implements RDBMSDataSourceResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(RDBMSDataSourceMap.class);

    private final Map<String, DataSourceConfiguration> datasources;
    private final Map<String, DataSourceConfiguration> databases;
    private final Map<String, DataSource> dbMap = new HashMap<>();
    private final Map<String, DataSource> dsMap = new HashMap<>();

    public RDBMSDataSourceMap(DataSourcesConfiguration ds) {
        Map<String, DataSourceConfiguration> x = ds.getDataSourcesByType(RDBMSDataSourceConfiguration.class);
        databases = new HashMap<>();
        datasources = new HashMap<>();
        for (DataSourceConfiguration cfg : x.values()) {
            String databaseName = ((RDBMSDataSourceConfiguration) cfg).getDatabaseName();
            databases.put(databaseName, cfg);
            List<String> dsList = ((RDBMSDataSourceConfiguration) cfg).getDataSourceName();
            for (String s : dsList){
                datasources.put(s, cfg);
            }
        }
    }

    public DataSource get(RDBMSDataStore store) {
        LOGGER.debug("Returning DataSource for {}", store);
        DataSource ds = null;
        try {
            if (store.getDatasourceName() != null) {
                LOGGER.debug("datasource:{}", store.getDatasourceName());
                ds = getAndPut(dsMap, datasources, store.getDatasourceName());
            } else if (store.getDatabaseName() != null) {
                LOGGER.debug("databaseName:{}", store.getDatabaseName());
                ds = getAndPut(dbMap, databases, store.getDatabaseName());
            }
        } catch (RuntimeException re) {
            LOGGER.error("Cannot get {}:{}", store, re);
            throw re;
        } catch (Exception e) {
            LOGGER.error("Cannot get {}:{}", store, e);
            throw new IllegalArgumentException(e);
        }
        if (ds == null) {
            throw new IllegalArgumentException("Cannot find DataSource for  " + store);
        }
        LOGGER.debug("Returning {} for {}", ds, store);
        return ds;
    }

    private DataSource getAndPut(Map<String, DataSource> mds, Map<String, DataSourceConfiguration> mdsc, String name) {
        DataSource ds = mds.get(name);
        if (ds == null) {
            RDBMSDataSourceConfiguration cfg = (RDBMSDataSourceConfiguration) mdsc.get(name);
            if (cfg == null) {
                throw new IllegalArgumentException("No datasource/database for " + name);
            }
            ds = cfg.getDataSource(name);
            mds.put(name, ds);
        }
        return ds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RDBMSDataSourceMap that = (RDBMSDataSourceMap) o;

        if (databases != null ? !databases.equals(that.databases) : that.databases != null) return false;
        if (datasources != null ? !datasources.equals(that.datasources) : that.datasources != null) return false;
        if (dbMap != null ? !dbMap.equals(that.dbMap) : that.dbMap != null) return false;
        if (dsMap != null ? !dsMap.equals(that.dsMap) : that.dsMap != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = datasources != null ? datasources.hashCode() : 0;
        result = 31 * result + (databases != null ? databases.hashCode() : 0);
        result = 31 * result + (dbMap != null ? dbMap.hashCode() : 0);
        result = 31 * result + (dsMap != null ? dsMap.hashCode() : 0);
        return result;
    }
}
