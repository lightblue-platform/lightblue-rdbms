package com.redhat.lightblue.config.rdbms;

import static org.junit.Assert.assertEquals;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.junit.Test;

import com.redhat.lightblue.common.rdbms.RDBMSDataStore;
import com.redhat.lightblue.config.DataSourcesConfiguration;

public class RDBMSDataSourceMapTest {

    private final int NUMBER = 123098;

    @Test
    public void testGet() throws Exception {
        DataSourcesConfiguration dsc = new DataSourcesConfiguration();
        RDBMSDataSourceConfiguration datasource = new RDBMSDataSourceConfiguration(){
            @Override
            public DataSource getDataSource(String name) {
                return new StubDataSource();
            }
        };
        datasource.setDatabaseName("testDB");
        datasource.setMetadataDataStoreParser(Class.forName("com.redhat.lightblue.metadata.rdbms.impl.RDBMSDataStoreParser"));
        dsc.add("test", datasource);
        datasource.getDataSourceJDNIMap().put("test","jndi");
        RDBMSDataSourceMap dsMap = new RDBMSDataSourceMap(dsc);
        RDBMSDataStore store = new RDBMSDataStore("testDB",null);

        DataSource dataSource = dsMap.get(store);
        assertEquals(new StubDataSource(), dataSource);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGet_DatasourceAndDatabaseNull() throws ClassNotFoundException{
        DataSourcesConfiguration dsc = new DataSourcesConfiguration();
        dsc.add("test", new RDBMSDataSourceConfiguration());

        RDBMSDataStore store = new RDBMSDataStore(null, null);
        new RDBMSDataSourceMap(dsc).get(store);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGet_NoDatasourceForName() throws ClassNotFoundException{
        DataSourcesConfiguration dsc = new DataSourcesConfiguration();
        dsc.add("test", new RDBMSDataSourceConfiguration());

        RDBMSDataStore store = new RDBMSDataStore(null, "fake");
        new RDBMSDataSourceMap(dsc).get(store);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGet_NoDatabaseForName() throws ClassNotFoundException{
        DataSourcesConfiguration dsc = new DataSourcesConfiguration();
        dsc.add("test", new RDBMSDataSourceConfiguration());

        RDBMSDataStore store = new RDBMSDataStore("fake", null);
        new RDBMSDataSourceMap(dsc).get(store);
    }

    private class StubDataSource implements DataSource {
        @Override public Connection getConnection() throws SQLException {return null;}
        @Override public Connection getConnection(String s, String s2) throws SQLException {return null;}
        @Override public PrintWriter getLogWriter() throws SQLException { return null; }
        @Override public void setLogWriter(PrintWriter printWriter) throws SQLException {}
        @Override public void setLoginTimeout(int i) throws SQLException {}
        @Override public int getLoginTimeout() throws SQLException {return 0;}
        @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {return null;}
        @Override public <T> T unwrap(Class<T> tClass) throws SQLException {return null;}
        @Override public boolean isWrapperFor(Class<?> aClass) throws SQLException {return false;}
        @Override public boolean equals(Object obj) {
            if(obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            return this.hashCode() == obj.hashCode();
        }
        @Override public int hashCode() {return NUMBER;}
    }
}