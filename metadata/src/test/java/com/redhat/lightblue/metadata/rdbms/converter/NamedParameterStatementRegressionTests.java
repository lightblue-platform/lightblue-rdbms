package com.redhat.lightblue.metadata.rdbms.converter;

import com.redhat.lightblue.metadata.rdbms.util.Column;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;

import static org.junit.Assert.*;

public class NamedParameterStatementRegressionTests {

    @Test
    public void testSQLGeneration() throws Exception {
        MyConnection connection;
        SQLConverter nps; // Basically just the NamedParameterStatement with more some internal methods exposed
        DynVar dynVar;
        RDBMSContext context = null;

        //Escape quotes tests
        {
            connection = new MyConnection();
            connection.myPreparedStatement = new MyPreparedStatement();
            nps = new SQLConverter(connection,"select * from my_table where  x = 'My name''s X' and y = :parameter");
            assertEquals("select * from my_table where  x = 'My name''s X' and y = ?",nps.processedQuery);
            assertFalse(nps.processedQuery.contains(":parameter"));
        }
        {
            connection = new MyConnection();
            connection.myPreparedStatement = new MyPreparedStatement();
            nps = new SQLConverter(connection,"select * from my_table where name = function(x, \"'S\", \"S\") and y = :parameter;");
            assertEquals(" ",nps.processedQuery);
            assertFalse(nps.processedQuery.contains(":parameter"));
        }
        //
    }

    static class SQLConverter extends NamedParameterStatement{
        public String processedQuery;
        public SQLConverter(Connection connection, String query) throws SQLException {
            super(connection, query);
        }
        @Override
        public String prepare(String query){
            processedQuery = super.prepare(query);
            return processedQuery;
        }
    }
}