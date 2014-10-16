package com.redhat.lightblue.common.rdbms;

import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.Test;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class RDBMSUtilsTest {

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