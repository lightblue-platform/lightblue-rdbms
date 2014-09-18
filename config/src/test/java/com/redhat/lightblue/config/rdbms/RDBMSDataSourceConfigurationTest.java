package com.redhat.lightblue.config.rdbms;

import com.fasterxml.jackson.databind.JsonNode;
import com.redhat.lightblue.metadata.rdbms.impl.RDBMSDataStoreParser;
import com.redhat.lightblue.util.JsonUtils;
import com.redhat.lightblue.util.test.FileUtil;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.naming.Context;
import javax.naming.InitialContext;
import java.io.InputStreamReader;

import static org.junit.Assert.*;

public class RDBMSDataSourceConfigurationTest {

    RDBMSDataSourceConfiguration cut;

    @After
    public void after(){
        cut = null;
    }


    @Before
    public void before() {
        cut = new RDBMSDataSourceConfiguration();
    }

    @Test
    public void testRDBMSDataSourceConfiguration() throws Exception {
        // Create initial context
        System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.apache.naming.java.javaURLContextFactory");
        System.setProperty(Context.URL_PKG_PREFIXES, "org.apache.naming");
        // already tried System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.jboss.as.naming.InitialContextFactory");
        InitialContext ic = new InitialContext();

        ic.createSubcontext("java:");
        ic.createSubcontext("java:/comp");
        ic.createSubcontext("java:/comp/env");
        ic.createSubcontext("java:/comp/env/jdbc");

        JdbcConnectionPool ds = JdbcConnectionPool.create("jdbc:h2:file:/tmp/test.db;FILE_LOCK=NO;MVCC=TRUE;DB_CLOSE_ON_EXIT=TRUE", "sa", "sasasa");

        ic.bind("java:/mydatasource", ds);

        JsonNode json = JsonUtils.json(FileUtil.readFile("rdbms-datasources.json"));
        JsonNode rdbmsNode = json.get("rdbms");
        cut.initializeFromJson(rdbmsNode);
        assertEquals("org.h2.jdbcx.JdbcConnectionPool",cut.getDataSource("datasource").getClass().getCanonicalName());
        assertEquals("datasource",cut.getDataSourceName().get(0));
        assertEquals("rdbms",cut.getDatabaseName());
        cut.setDatabaseName("db");
        assertEquals("db", cut.getDatabaseName());
        cut.setMetadataDataStoreParser(RDBMSDataStoreParser.class);
        assertEquals("com.redhat.lightblue.metadata.rdbms.impl.RDBMSDataStoreParser",cut.getMetadataDataStoreParser().getCanonicalName());
    }

    @Test
    public void testEqualsHashCodeToString() throws Exception {
        RDBMSDataSourceConfiguration test = new RDBMSDataSourceConfiguration();
        assertTrue(test.equals(cut));
        assertEquals(test.hashCode(),cut.hashCode());
        assertEquals(test.toString(),cut.toString());
    }


}