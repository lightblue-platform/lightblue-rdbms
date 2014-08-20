package com.redhat.lightblue.tools.rdbms;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.Before;
import org.junit.Test;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ITCaseRDBMSToolsTest {
    static
    {
        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.INFO);
        rootLogger.addAppender(new ConsoleAppender(
                new PatternLayout("%-6r [%p] %c - %m%n")));
    }

    @Before
    public void setup() throws Exception {
        File folder = new File("/tmp");
        File[] files = folder.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(final File dir, final String name) {
                return name.startsWith("test.db");
            }
        });
        for (final File file : files) {
            if (!file.delete()) {
                System.out.println("Failed to remove " + file.getAbsolutePath());
            }
        }

        try {
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
        } catch (NamingException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Test
    public void testFirstIntegrationTest() throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, URISyntaxException {
        try {
            Context initCtx = new InitialContext();
            DataSource ds = (DataSource) initCtx.lookup("java:/mydatasource");
            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE People ( PersonID int, Name varchar(255) );");

            // Good resource for examples of procedure with h2 https://code.google.com/p/h2database/source/browse/trunk/h2/src/test/org/h2/samples/Function.java
            stmt.execute("CREATE ALIAS getVersion FOR \"org.h2.engine.Constants.getVersion\"");
            ResultSet rs = stmt.executeQuery("CALL getVersion()");
            if (rs.next()) {
                System.out.println("Version: " + rs.getString(1));
            }
            stmt.close();
            File json = new File("/tmp/rdbms.json");
            if (!json.delete()) {
                System.out.println("Failed to remove " + json.getAbsolutePath());
            }


            RDBMSTools.main(new String[]{});

        } catch (NamingException ex) {
            throw new IllegalStateException(ex);
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }
}