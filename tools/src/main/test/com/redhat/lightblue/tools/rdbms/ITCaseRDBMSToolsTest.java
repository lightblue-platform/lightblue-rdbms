package com.redhat.lightblue.tools.rdbms;

import org.h2.jdbcx.JdbcConnectionPool;
import org.json.JSONException;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class ITCaseRDBMSToolsTest {
    public static boolean singletonInit = true;
    public static final String outputFile = "/tmp/rdbms.json";

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
        if(singletonInit) {
            singletonInit = false;
            try {
                // Create initial context
                System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.apache.naming.java.javaURLContextFactory");
                System.setProperty(Context.URL_PKG_PREFIXES, "org.apache.naming");
                // already tried System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.jboss.as.naming.InitialContextFactory");
                InitialContext ic = new InitialContext();

                try {
                    ic.destroySubcontext("java:");
                } catch (NamingException ex) {
                    //ignore
                }
                ic.createSubcontext("java:");


                JdbcConnectionPool jdbcs = JdbcConnectionPool.create("jdbc:h2:file:/tmp/test.db;FILE_LOCK=NO;MVCC=TRUE;DB_CLOSE_ON_EXIT=TRUE", "sa", "sasasa");

                ic.bind("java:/mydatasource", jdbcs);

                Context initCtx = new InitialContext();
                DataSource ds = (DataSource) initCtx.lookup("java:/mydatasource");
                Connection conn = ds.getConnection();
                RDBMSTools.createTables(conn);
                initCtx.close();
            } catch (NamingException ex) {
                throw new IllegalStateException(ex);
            }
        }
    }

    @Test
    public void testJoinsIntegrationTest() throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, URISyntaxException {
        try {
            File json = new File(outputFile);
            if (!json.delete()) {
                System.out.println("Failed to remove " + json.getAbsolutePath());
            }

            RDBMSTools.main(new String[]{"hibernateDialect=org.hibernate.dialect.H2Dialect","jndi=java:/mydatasource","outputFile="+outputFile,"strategyCanonicalName=com.redhat.lightblue.tools.rdbms.JoinedTablesSQLMappingTranslator"});

            String result = Files.readAllLines(Paths.get(outputFile), Charset.defaultCharset()).get(0);
            String expected = "{\"rdbms\":{\"fetch\":{\"expressions\":[{\"$statement\":{\"sql\":\"select * from NEED_TO_CHANGE\",\"type\":\"select\"}}]},\"dialect\":\"oracle\",\"SQLMapping\":{\"joins\":[{\"tables\":[{\"name\":\"DOCUMENT\"},{\"name\":\"PEOPLE\"}],\"joinTablesStatement\":\"DOCUMENT.PERSONID=PEOPLE.PERSONID\",\"projectionMappings\":[{\"column\":\"DOCID\",\"field\":\"DOCID\",\"sort\":\"DOCID\"},{\"column\":\"PERSONID\",\"field\":\"PERSONID\",\"sort\":\"PERSONID\"},{\"column\":\"NAME\",\"field\":\"NAME\",\"sort\":\"NAME\"}]}],\"columnToFieldMap\":[{\"table\":\"DOCUMENT\",\"column\":\"DOCID\",\"field\":\"DOCID\"},{\"table\":\"PEOPLE\",\"column\":\"PERSONID\",\"field\":\"PERSONID\"},{\"table\":\"PEOPLE\",\"column\":\"NAME\",\"field\":\"NAME\"}]}}}";
            System.out.println(result);
            JSONAssert.assertEquals(expected, result, false);

        } catch ( JSONException | IOException ex ) {
            throw new IllegalStateException(ex);
        }
    }

    @Test
    public void testSimpleIntegrationTest() {
        try {
            File json = new File(outputFile);
            if (!json.delete()) {
                System.out.println("Failed to remove " + json.getAbsolutePath());
            }

            RDBMSTools.main(new String[]{"hibernateDialect=org.hibernate.dialect.H2Dialect","jndi=java:/mydatasource","outputFile="+outputFile});

            String result = Files.readAllLines(Paths.get(outputFile), Charset.defaultCharset()).get(0);
            String expected = "{\"rdbms\":{\"fetch\":{\"expressions\":[{\"$statement\":{\"sql\":\"select * from NEED_TO_CHANGE\",\"type\":\"select\"}}]},\"dialect\":\"oracle\",\"SQLMapping\":{\"joins\":[{\"tables\":[{\"name\":\"DOCUMENT\"}],\"joinTablesStatement\":null,\"projectionMappings\":[{\"column\":\"DOCID\",\"field\":\"DOCID\",\"sort\":\"DOCID\"}]},{\"tables\":[{\"name\":\"PEOPLE\"}],\"joinTablesStatement\":null,\"projectionMappings\":[{\"column\":\"PERSONID\",\"field\":\"PERSONID\",\"sort\":\"PERSONID\"},{\"column\":\"NAME\",\"field\":\"NAME\",\"sort\":\"NAME\"}]}],\"columnToFieldMap\":[{\"table\":\"DOCUMENT\",\"column\":\"DOCID\",\"field\":\"DOCID\"},{\"table\":\"PEOPLE\",\"column\":\"PERSONID\",\"field\":\"PERSONID\"},{\"table\":\"PEOPLE\",\"column\":\"NAME\",\"field\":\"NAME\"}]}}}";
            //System.out.println(result);
            JSONAssert.assertEquals(expected, result, false);

        } catch ( JSONException | IOException ex ) {
            throw new IllegalStateException(ex);
        }
    }
}