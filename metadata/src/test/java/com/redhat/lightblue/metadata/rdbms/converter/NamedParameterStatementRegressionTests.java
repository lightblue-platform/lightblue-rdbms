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
            assertFalse(nps.processedQuery.contains(":parameter"));
        }
        {
            connection = new MyConnection();
            connection.myPreparedStatement = new MyPreparedStatement();
            nps = new SQLConverter(connection,"select * from my_table where name = function(x, \"'S\", \"S\") and surname = function(y, '\"X\"', \"X\") and y = :parameter;");
            assertEquals("select * from my_table where name = function(x, \"'S\", \"S\") and surname = function(y, '\"X\"', \"X\") and y = ?;",nps.processedQuery);
            assertFalse(nps.processedQuery.contains(":parameter"));
        }
        {
            connection = new MyConnection();
            connection.myPreparedStatement = new MyPreparedStatement();
            nps = new SQLConverter(connection,"insert into my_table values (\"the test's working.\", :parameter);");
            assertEquals("insert into my_table values (\"the test's working.\", ?);",nps.processedQuery);
            assertFalse(nps.processedQuery.contains(":parameter"));
        }
        {
            connection = new MyConnection();
            connection.myPreparedStatement = new MyPreparedStatement();
            nps = new SQLConverter(connection,"insert into my_table values ('hi, my name'+chr(39)+'s tim.', :parameter);");
            assertEquals("insert into my_table values ('hi, my name'+chr(39)+'s tim.', ?);",nps.processedQuery);
            assertFalse(nps.processedQuery.contains(":parameter"));
        }
        {
            connection = new MyConnection();
            connection.myPreparedStatement = new MyPreparedStatement();
            nps = new SQLConverter(connection,"SELECT * FROM TableName WHERE x = :parameter and FieldName = replace(\"ProNumber\", \"'\", \"''\")  and y = :parameter ");
            assertEquals("SELECT * FROM TableName WHERE x = ? and FieldName = replace(\"ProNumber\", \"'\", \"''\")  and y = ? ", nps.processedQuery);
            assertFalse(nps.processedQuery.contains(":parameter"));
        }

        // : or ' is used as an escape in LIKE
        {
            connection = new MyConnection();
            connection.myPreparedStatement = new MyPreparedStatement();
            nps = new SQLConverter(connection,"select * from my_table where field like '%''something''%' and other = :parameter");
            assertEquals("select * from my_table where field like '%''something''%' and other = ?", nps.processedQuery);
            assertFalse(nps.processedQuery.contains(":parameter"));
        }
        {
            connection = new MyConnection();
            connection.myPreparedStatement = new MyPreparedStatement();
            nps = new SQLConverter(connection,"select * from my_table where field like '%:otherthing%' and other = :parameter");
            assertEquals("select * from my_table where field like '%:otherthing%' and other = ?", nps.processedQuery);
            assertFalse(nps.processedQuery.contains(":parameter"));
        }

        //check if parser assumes anything that starts with : is a variable
        {
            connection = new MyConnection();
            connection.myPreparedStatement = new MyPreparedStatement();
            nps = new SQLConverter(connection,"select \"address:city\" from  \"places\"");
            assertEquals("select \"address:city\" from  \"places\"", nps.processedQuery);
            assertFalse(nps.processedQuery.contains(":parameter"));
        }

        //malformed SQL
        {
            connection = new MyConnection();
            connection.myPreparedStatement = new MyPreparedStatement();
            nps = new SQLConverter(connection,"select:");
            assertEquals("select:", nps.processedQuery);
            assertFalse(nps.processedQuery.contains(":parameter"));
        }
        {
            connection = new MyConnection();
            connection.myPreparedStatement = new MyPreparedStatement();
            nps = new SQLConverter(connection,"select : "); // '=' and ' ' are ignored, so it will not convert to '?'
            assertEquals("select : ", nps.processedQuery);
            assertFalse(nps.processedQuery.contains(":parameter"));
        }

        //others "edge" cases
        {
            connection = new MyConnection();
            connection.myPreparedStatement = new MyPreparedStatement();
            nps = new SQLConverter(connection,"select * from table where column like 'abc:%def%' escape ':'");
            assertEquals("select * from table where column like 'abc:%def%' escape ':'", nps.processedQuery);
            assertFalse(nps.processedQuery.contains("?")); //it should not find a field
        }
        {
            connection = new MyConnection();
            connection.myPreparedStatement = new MyPreparedStatement();
            nps = new SQLConverter(connection,"select * from table where column like ' 123 \"123\"123 '");
            assertEquals("select * from table where column like ' 123 \"123\"123 '", nps.processedQuery);
            assertFalse(nps.processedQuery.contains("?")); //it should not find a field
        }
        {
            connection = new MyConnection();
            connection.myPreparedStatement = new MyPreparedStatement();
            nps = new SQLConverter(connection,"select * from table where column like ' 123 \\\"123\\\"1 :xyz 23 '");
            assertEquals("select * from table where column like ' 123 \\\"123\\\"1 :xyz 23 '", nps.processedQuery);
            assertFalse(nps.processedQuery.contains("?")); //it should not find a field
        }


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