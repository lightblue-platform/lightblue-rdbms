package com.redhat.lightblue.hystrix.rdbms;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.Mongo;

import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.metadata.rdbms.converter.RDBMSContext;
import com.redhat.lightblue.metadata.rdbms.converter.RowMapper;
import com.redhat.lightblue.metadata.rdbms.converter.SelectStmt;
import com.redhat.lightblue.metadata.rdbms.converter.Translator;
import com.redhat.lightblue.metadata.rdbms.util.Column;
import com.redhat.lightblue.util.JsonUtils;
import static com.redhat.lightblue.util.test.FileUtil.readFile;
import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.RuntimeConfigBuilder;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.config.io.ProcessOutput;
import de.flapdoodle.embed.process.io.IStreamProcessor;
import de.flapdoodle.embed.process.io.Processors;
import de.flapdoodle.embed.process.runtime.Network;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import javax.inject.Inject;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import junit.framework.*;
import org.h2.jdbcx.JdbcConnectionPool;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.json.JSONException;
import org.junit.*;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.skyscreamer.jsonassert.JSONAssert;

import static org.junit.Assert.*;

public class ExecuteSQLCommandTest {

    private static boolean notRegistered = true;

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

        if(notRegistered) {
            notRegistered = false;
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
        } else {
            Context initCtx = new InitialContext();
            DataSource ds = (DataSource) initCtx.lookup("java:/mydatasource");
            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();
            stmt.execute("DROP ALL OBJECTS ");
            stmt.close();
        }
    }

    @Test
    public void testExecute() throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, URISyntaxException, JSONException {
        try {
            Context initCtx = new InitialContext();
            DataSource ds = (DataSource) initCtx.lookup("java:/mydatasource");
            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE Country ( name varchar(255), iso2code varchar(255), iso3code varchar(255) );");
            stmt.execute("INSERT INTO Country (name,iso2code,iso3code) VALUES ('a','CA','c');");
            stmt.close();
            conn.close();

            RDBMSContext rdbmsContext = new RDBMSContext();
            rdbmsContext.setRowMapper(new TestRowMapper(rdbmsContext));
            ds = (DataSource) initCtx.lookup("java:/mydatasource");
            rdbmsContext.setDataSource(ds);
            rdbmsContext.setSql("SELECT name,iso2code,iso3code from COUNTRY WHERE iso2code ='CA'");
            rdbmsContext.setType("select");
            new ExecuteSQLCommand(rdbmsContext).execute();
            assertEquals("a",rdbmsContext.getOutVar().getValues("name").get(0));
            assertEquals("CA",rdbmsContext.getOutVar().getValues("iso2code").get(0));
            assertEquals("c",rdbmsContext.getOutVar().getValues("iso3code").get(0));
        } catch (NamingException ex) {
            throw new IllegalStateException(ex);
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }


    @Test
    public void testExecuteList() throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, URISyntaxException, JSONException {
        try {
            Context initCtx = new InitialContext();
            DataSource ds = (DataSource) initCtx.lookup("java:/mydatasource");
            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE Country ( name varchar(255), iso2code varchar(255), iso3code varchar(255) );");
            stmt.execute("INSERT INTO Country (name,iso2code,iso3code) VALUES ('a','CA','c');");
            stmt.close();
            conn.close();

            ArrayList<SelectStmt> list = new ArrayList<>();
            list.add(new TestSelectStmt("SELECT name,iso2code,iso3code from COUNTRY WHERE iso2code ='CA'"));
            RDBMSContext rdbmsContext = new RDBMSContext();
            rdbmsContext.setRowMapper(new TestRowMapper(rdbmsContext));
            ds = (DataSource) initCtx.lookup("java:/mydatasource");
            rdbmsContext.setDataSource(ds);
            new ExecuteSQLCommand(rdbmsContext,list).execute();
            assertEquals("a",rdbmsContext.getOutVar().getValues("name").get(0));
            assertEquals("CA",rdbmsContext.getOutVar().getValues("iso2code").get(0));
            assertEquals("c",rdbmsContext.getOutVar().getValues("iso3code").get(0));
        } catch (NamingException ex) {
            throw new IllegalStateException(ex);
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private static class TestRowMapper implements RowMapper {
        private final RDBMSContext rdbmsContext;

        public TestRowMapper(RDBMSContext rdbmsContext) {
            this.rdbmsContext = rdbmsContext;
        }

        @Override
        public Object map(ResultSet resultSet) throws SQLException {
            String name = resultSet.getString(1);
            rdbmsContext.getOutVar().put(name, String.class, Column.createTemp("name", String.class.getCanonicalName()));
            String iso2code = resultSet.getString(2);
            rdbmsContext.getOutVar().put(iso2code, String.class, Column.createTemp("iso2code", String.class.getCanonicalName()));
            String iso3code = resultSet.getString(3);
            rdbmsContext.getOutVar().put(iso3code, String.class, Column.createTemp("iso3code", String.class.getCanonicalName()));
            return null;
        }
    }

    private static class TestSelectStmt extends SelectStmt {
        String statement;
        public TestSelectStmt(Translator t) {
            super(t);
        }
        public TestSelectStmt(String statement){
            super(null);
            this.statement=statement;
        }

        @Override
        public String generateStatement() {
            return statement;
        }
    }
}