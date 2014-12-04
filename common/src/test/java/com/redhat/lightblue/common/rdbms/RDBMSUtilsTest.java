package com.redhat.lightblue.common.rdbms;

import static org.junit.Assert.assertEquals;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class RDBMSUtilsTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testGetDatasSource_DataSourceNotFound(){
        expectedEx.expect(com.redhat.lightblue.util.Error.class);
        expectedEx.expectMessage("{\"objectType\":\"error\",\"context\":\"RDBMSUtils/getDataSource\",\"errorCode\":\""
                + RDBMSConstants.ERR_DATASOURCE_NOT_FOUND
                + "\",\"msg\":\"Need to specify class name in environment or system property, or as an applet parameter, or in an application resource file:  java.naming.factory.initial\"}");

        RDBMSUtils.getDataSource("Fake Data Source that was never defined");
    }

    @Test
    public void testGetDataSource() throws Exception {
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

        String jndiName = "java:/mydatasource";
        ic.bind(jndiName, ds);


        DataSource dataSource = RDBMSUtils.getDataSource(jndiName);
        assertEquals("org.h2.jdbcx.JdbcConnectionPool",dataSource.getClass().getCanonicalName());

    }
}