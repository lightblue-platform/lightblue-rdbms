package com.redhat.lightblue.metadata.rdbms.converter;

import com.redhat.lightblue.metadata.rdbms.util.Column;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.*;

import static org.junit.Assert.*;

public class RDBMSUtilsMetadataTest {

    @Test
    public void testProcessDynVar() throws Exception {
        MyConnection connection;
        NamedParameterStatement nps;
        DynVar dynVar;
        RDBMSContext context = null;
        final Flag flag = new Flag();

        {
            flag.reset();
            final Boolean value = new Boolean(true);
            final Class<?> objectClass = Boolean.class;
            connection = new MyConnection();
            connection.myPreparedStatement = new MyPreparedStatement() {
                @Override
                public void setBoolean(int parameterIndex, boolean x) throws SQLException {
                    assertEquals(1, parameterIndex);
                    assertEquals(value.booleanValue(), x);
                    flag.done();
                }
            };
            nps = new NamedParameterStatement(connection, ":parameter");
            dynVar = new DynVar(context);
            Column column = new Column(0, "parameter", "parameter", "java.lang.Boolean", Types.INTEGER);
            dynVar.put(value, objectClass, column);
            RDBMSUtilsMetadata.processDynVar(context, nps, dynVar);
            assertTrue(flag.isAsserted());
        }

        {
            flag.reset();
            final Short value = new Short((short) 26);
            final Class<?> objectClass = Short.class;
            connection = new MyConnection();
            connection.myPreparedStatement = new MyPreparedStatement() {
                @Override
                public void setInt(int parameterIndex, int x) throws SQLException {
                    assertEquals(1, parameterIndex);
                    assertEquals(value.shortValue(), x);
                    flag.done();
                }
            };
            nps = new NamedParameterStatement(connection, ":parameter");
            dynVar = new DynVar(context);
            Column column = new Column(0, "parameter", "parameter", "java.lang.Short", Types.INTEGER);
            dynVar.put(value, objectClass, column);
            RDBMSUtilsMetadata.processDynVar(context, nps, dynVar);
            assertTrue(flag.isAsserted());
        }

        {
            flag.reset();
            final Integer value = new Integer(256);
            final Class<?> objectClass = Integer.class;
            connection = new MyConnection();
            connection.myPreparedStatement = new MyPreparedStatement() {
                @Override
                public void setInt(int parameterIndex, int x) throws SQLException {
                    assertEquals(1, parameterIndex);
                    assertEquals(value.intValue(), x);
                    flag.done();
                }
            };
            nps = new NamedParameterStatement(connection, ":parameter");
            dynVar = new DynVar(context);
            Column column = new Column(0, "parameter", "parameter", "java.lang.Integer", Types.INTEGER);
            dynVar.put(value, objectClass, column);
            RDBMSUtilsMetadata.processDynVar(context, nps, dynVar);
            assertTrue(flag.isAsserted());
        }

        {
            flag.reset();
            final Long value = new Long(4789456123L);
            final Class<?> objectClass = Long.class;
            connection = new MyConnection();
            connection.myPreparedStatement = new MyPreparedStatement() {
                @Override
                public void setLong(int parameterIndex, long x) throws SQLException {
                    assertEquals(1, parameterIndex);
                    assertEquals(value.longValue(), x);
                    flag.done();
                }
            };
            nps = new NamedParameterStatement(connection, ":parameter");
            dynVar = new DynVar(context);
            Column column = new Column(0, "parameter", "parameter", "java.lang.Long", Types.INTEGER);
            dynVar.put(value, objectClass, column);
            RDBMSUtilsMetadata.processDynVar(context, nps, dynVar);
            assertTrue(flag.isAsserted());
        }

        {
            flag.reset();
            final Double value = new Double(2.222D);
            final Class<?> objectClass = Double.class;
            connection = new MyConnection();
            connection.myPreparedStatement = new MyPreparedStatement() {
                @Override
                public void setDouble(int parameterIndex, double x) throws SQLException {
                    assertEquals(1, parameterIndex);
                    assertEquals(value.doubleValue(), x, 0.00001D);
                    flag.done();
                }
            };
            nps = new NamedParameterStatement(connection, ":parameter");
            dynVar = new DynVar(context);
            Column column = new Column(0, "parameter", "parameter", "java.lang.Double", Types.INTEGER);
            dynVar.put(value, objectClass, column);
            RDBMSUtilsMetadata.processDynVar(context, nps, dynVar);
            assertTrue(flag.isAsserted());
        }

        {
            flag.reset();
            final String value = "Test";
            final Class<?> objectClass = String.class;
            connection = new MyConnection();
            connection.myPreparedStatement = new MyPreparedStatement() {
                @Override
                public void setString(int parameterIndex, String x) throws SQLException {
                    assertEquals(1, parameterIndex);
                    assertEquals(value, x);
                    flag.done();
                }
            };
            nps = new NamedParameterStatement(connection, ":parameter");
            dynVar = new DynVar(context);
            Column column = new Column(0, "parameter", "parameter", "java.lang.String", Types.INTEGER);
            dynVar.put(value, objectClass, column);
            RDBMSUtilsMetadata.processDynVar(context, nps, dynVar);
            assertTrue(flag.isAsserted());
        }

        {
            flag.reset();
            final java.util.Date value = new java.util.Date();
            final Class<?> objectClass = java.util.Date.class;
            connection = new MyConnection();
            connection.myPreparedStatement = new MyPreparedStatement() {
                @Override
                public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
                    assertEquals(1, parameterIndex);
                    assertEquals(value.getTime(), x.getTime());
                    flag.done();
                }
            };
            nps = new NamedParameterStatement(connection, ":parameter");
            dynVar = new DynVar(context);
            Column column = new Column(0, "parameter", "parameter", "java.util.Date", Types.INTEGER);
            dynVar.put(value, objectClass, column);
            RDBMSUtilsMetadata.processDynVar(context, nps, dynVar);
            assertTrue(flag.isAsserted());
        }

        {
            flag.reset();
            final java.sql.Date value = new java.sql.Date(109228L);
            final Class<?> objectClass = java.sql.Date.class;
            connection = new MyConnection();
            connection.myPreparedStatement = new MyPreparedStatement() {
                @Override
                public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
                    assertEquals(1, parameterIndex);
                    assertEquals(value.getTime(), x.getTime());
                    flag.done();
                }
            };
            nps = new NamedParameterStatement(connection, ":parameter");
            dynVar = new DynVar(context);
            Column column = new Column(0, "parameter", "parameter", "java.sql.Date", Types.INTEGER);
            dynVar.put(value, objectClass, column);
            RDBMSUtilsMetadata.processDynVar(context, nps, dynVar);
            assertTrue(flag.isAsserted());
        }

        {
            flag.reset();
            final java.sql.Date value = new java.sql.Date(109228L);
            final Class<?> objectClass = java.sql.Date.class;
            connection = new MyConnection();
            connection.myPreparedStatement = new MyPreparedStatement() {
                @Override
                public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
                    assertEquals(1, parameterIndex);
                    assertEquals(value.getTime(), x.getTime());
                    flag.done();
                }
            };
            nps = new NamedParameterStatement(connection, ":parameter");
            dynVar = new DynVar(context);
            Column column = new Column(0, "parameter", "parameter", "java.sql.Date", Types.INTEGER);
            dynVar.put(value, objectClass, column);
            RDBMSUtilsMetadata.processDynVar(context, nps, dynVar);
            assertTrue(flag.isAsserted());
        }

        {
            flag.reset();
            final Time value = new Time(2321312L);
            final Class<?> objectClass = java.sql.Time.class;
            connection = new MyConnection();
            connection.myPreparedStatement = new MyPreparedStatement() {
                @Override
                public void setTime(int parameterIndex, Time x) throws SQLException {
                    assertEquals(1, parameterIndex);
                    assertEquals(value.getTime(), x.getTime());
                    flag.done();
                }
            };
            nps = new NamedParameterStatement(connection, ":parameter");
            dynVar = new DynVar(context);
            Column column = new Column(0, "parameter", "parameter", "java.sql.Time", Types.INTEGER);
            dynVar.put(value, objectClass, column);
            RDBMSUtilsMetadata.processDynVar(context, nps, dynVar);
            assertTrue(flag.isAsserted());
        }


        {
            flag.reset();
            final byte[] value = new byte[]{};
            final Class<?> objectClass = value.getClass();
            connection = new MyConnection();
            connection.myPreparedStatement = new MyPreparedStatement() {
                @Override
                public void setBytes(int parameterIndex, byte[] x) throws SQLException {
                    assertEquals(1, parameterIndex);
                    assertEquals(value, x);
                    flag.done();
                }
            };
            nps = new NamedParameterStatement(connection, ":parameter");
            dynVar = new DynVar(context);
            Column column = new Column(0, "parameter", "parameter", "java.sql.Time", Types.INTEGER);
            dynVar.put(value, objectClass, column);
            RDBMSUtilsMetadata.processDynVar(context, nps, dynVar);
            assertTrue(flag.isAsserted());
        }

        {
            flag.reset();
            final java.math.BigDecimal value = new java.math.BigDecimal(456123L);
            final Class<?> objectClass = java.math.BigDecimal.class;
            connection = new MyConnection();
            connection.myPreparedStatement = new MyPreparedStatement() {
                @Override
                public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
                    assertEquals(1, parameterIndex);
                    assertEquals(value, x);
                    flag.done();
                }
            };
            nps = new NamedParameterStatement(connection, ":parameter");
            dynVar = new DynVar(context);
            Column column = new Column(0, "parameter", "parameter", "java.math.BigDecimal", Types.DECIMAL);
            dynVar.put(value, objectClass, column);
            RDBMSUtilsMetadata.processDynVar(context, nps, dynVar);
            assertTrue(flag.isAsserted());
        }
    }


}
