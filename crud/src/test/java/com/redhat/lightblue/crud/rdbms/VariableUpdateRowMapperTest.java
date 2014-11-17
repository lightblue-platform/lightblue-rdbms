package com.redhat.lightblue.crud.rdbms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.lang.model.type.NullType;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.redhat.lightblue.crud.rdbms.VariableUpdateRowMapperTest.EasyValueChecker;
import com.redhat.lightblue.crud.rdbms.VariableUpdateRowMapperTest.NonStandardChecker;
import com.redhat.lightblue.crud.rdbms.VariableUpdateRowMapperTest.UnsupportedOperationExceptionChecker;
import com.redhat.lightblue.metadata.rdbms.converter.DynVar;
import com.redhat.lightblue.metadata.rdbms.converter.RDBMSContext;

@RunWith(Suite.class)
@SuiteClasses(value={EasyValueChecker.class, UnsupportedOperationExceptionChecker.class, NonStandardChecker.class})
public class VariableUpdateRowMapperTest {

    private final static String LABEL = "FakeColumnLabel";

    public static ResultSet createMockedResultSet(String name, int type)
            throws SQLException {
        ResultSetMetaData rsmd = mock(ResultSetMetaData.class);
        when(rsmd.getColumnCount()).thenReturn(1);
        when(rsmd.getColumnName(1)).thenReturn(name);
        when(rsmd.getColumnLabel(1)).thenReturn(LABEL);
        when(rsmd.getColumnClassName(1)).thenReturn("FakeColumnClass");
        when(rsmd.getColumnType(1)).thenReturn(type);

        ResultSet mockResultSet = mock(ResultSet.class);
        when(mockResultSet.getMetaData()).thenReturn(rsmd);

        return mockResultSet;
    }

    /**
     * Tests that are repeatable for the bulk of the types.
     */
    @RunWith(Parameterized.class)
    public static class EasyValueChecker{

        /**
         * Col1: For debugging purposes to know which test case had issues
         * Col2: The {@link Types} value to test with.
         * Col3: The value that should be returned from the {@link ResultSet}
         */
        @Parameters(name = "{index}: {0}")
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][] {
                    {"BIT", Types.BIT, true},
                    {"BOOLEAN", Types.BOOLEAN, true},
                    {"TINYINT", Types.TINYINT, new Integer(1).shortValue()},
                    {"SMALLINT", Types.SMALLINT, new Integer(1).shortValue()},
                    {"INTEGER", Types.INTEGER, 4},
                    {"BIGINT", Types.BIGINT, 4L},
                    {"ROWID", Types.ROWID, 4L},
                    {"FLOAT", Types.FLOAT, 4D},
                    {"REAL", Types.REAL, 4D},
                    {"DOUBLE", Types.DOUBLE, 4D},
                    {"NUMERIC", Types.NUMERIC, new BigDecimal(4)},
                    {"DECIMAL", Types.DECIMAL, new BigDecimal(4)},
                    {"CHAR", Types.CHAR, "A"},
                    {"VARCHAR", Types.VARCHAR, "hello"},
                    {"LONGVARCHAR", Types.LONGVARCHAR, "hello"},
                    {"NCHAR", Types.NCHAR, "N"},
                    {"NVARCHAR", Types.NVARCHAR, "hello"},
                    {"LONGNVARCHAR", Types.LONGNVARCHAR, "hello"},
                    {"DATE", Types.DATE, new Date(System.currentTimeMillis())},
                    {"TIME", Types.TIME, new Time(System.currentTimeMillis())}
                    //{"JAVA_OBJECT", Types.JAVA_OBJECT, new Object()}
            });
        }

        private final Object instanceToReturn;
        private final int type;

        private final VariableUpdateRowMapper variableUpdateRowMapper;
        private final RDBMSContext rdbmsContext;
        private final ResultSet mockResultSet;

        /**
         * Because this class is using the Parameterized runner, this constructor will act as
         * if it was @Before
         */
        public EasyValueChecker(String name, int type, Object instanceToReturn)
                throws SQLException{
            this.instanceToReturn = instanceToReturn;
            this.type = type;

            rdbmsContext = new RDBMSContext();
            DynVar dyn = new DynVar(rdbmsContext);
            rdbmsContext.setInVar(dyn);

            variableUpdateRowMapper = new VariableUpdateRowMapper(rdbmsContext);

            mockResultSet = createMockedResultSet(name, type);
            mockOutAppropriateGetMethod();
        }

        /**
         * This method is unfortunate, but I could find no other way to mock out the
         * appropriate method being called.
         */
        private void mockOutAppropriateGetMethod() throws SQLException{
            switch (type){
                case Types.BIT:
                case Types.BOOLEAN:
                    when(mockResultSet.getBoolean(1)).thenReturn((Boolean) instanceToReturn);
                    break;

                case Types.TINYINT:
                case Types.SMALLINT:
                    when(mockResultSet.getShort(1)).thenReturn((Short) instanceToReturn);
                    break;

                case Types.INTEGER:
                    when(mockResultSet.getInt(1)).thenReturn((Integer) instanceToReturn);
                    break;

                case Types.BIGINT:
                case Types.ROWID:
                    when(mockResultSet.getLong(1)).thenReturn((Long) instanceToReturn);
                    break;

                case Types.FLOAT:
                case Types.REAL:
                case Types.DOUBLE:
                    when(mockResultSet.getDouble(1)).thenReturn((Double) instanceToReturn);
                    break;

                case Types.NUMERIC:
                case Types.DECIMAL:
                    when(mockResultSet.getBigDecimal(1)).thenReturn((BigDecimal) instanceToReturn);
                    break;

                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                    when(mockResultSet.getString(1)).thenReturn(instanceToReturn.toString());
                    break;

                case Types.DATE:
                    when(mockResultSet.getDate(1)).thenReturn((Date) instanceToReturn);
                    break;

                case Types.TIME:
                    when(mockResultSet.getTime(1)).thenReturn((Time) instanceToReturn);
                    break;

                case Types.JAVA_OBJECT:
                    when(mockResultSet.getObject(1)).thenReturn(instanceToReturn);
                    break;
            }
        }

        @Test
        public void testMap_Null() throws SQLException{
            when(mockResultSet.wasNull()).thenReturn(true);

            variableUpdateRowMapper.map(mockResultSet);

            DynVar dynVar = rdbmsContext.getOutVar();

            assertNotNull(dynVar);
            assertTrue(dynVar.getFirstClassFromKey(LABEL).isAssignableFrom(instanceToReturn.getClass()));

            List values = dynVar.getValues(LABEL);
            assertNotNull(values);
            assertEquals(1, values.size());

            Object value = values.get(0);
            assertNull(value);
        }

        @Test
        public void testMap_NotNull() throws SQLException{
            when(mockResultSet.wasNull()).thenReturn(false);

            variableUpdateRowMapper.map(mockResultSet);

            DynVar dynVar = rdbmsContext.getOutVar();

            assertNotNull(dynVar);
            assertTrue(dynVar.getFirstClassFromKey(LABEL).isAssignableFrom(instanceToReturn.getClass()));

            List values = dynVar.getValues(LABEL);
            assertNotNull(values);
            assertEquals(1, values.size());

            Object value = values.get(0);
            assertEquals(instanceToReturn, value);
        }

    }

    /**
     * These types are not currently supported and should throw an
     * {@link UnsupportedOperationException}.
     */
    @RunWith(Parameterized.class)
    public static class UnsupportedOperationExceptionChecker{

        /**
         * Col1: For debugging purposes to know which test case had issues
         * Col2: The {@link Types} value to test with.
         */
        @Parameters(name = "{index}: {0}")
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][] {
                    {"OTHER", Types.OTHER},
                    {"DISTRICT", Types.DISTINCT},
                    {"STRUCT", Types.STRUCT},
                    {"ARRAY", Types.ARRAY},
                    {"REF", Types.REF},
                    {"DATALINK", Types.DATALINK},
                    {"NCLOB", Types.NCLOB},
                    {"SQLXML", Types.SQLXML},

                    /*
                     * This is the ony entry that is not an actual type. The idea being
                     * to test the default case.
                     */
                    {"FAKE", 123456789}
            });
        }

        private final int type;

        private final VariableUpdateRowMapper variableUpdateRowMapper;
        private final ResultSet mockResultSet;

        public UnsupportedOperationExceptionChecker(String name, int type) throws SQLException{
            this.type = type;

            variableUpdateRowMapper = new VariableUpdateRowMapper(new RDBMSContext());

            mockResultSet = createMockedResultSet(name, type);
        }

        @Test(expected = UnsupportedOperationException.class)
        public void testMap_Null() throws SQLException{
            variableUpdateRowMapper.map(mockResultSet);
        }
    }

    /**
     * Remaining test cases that are unique in some way and can't be done generically.
     */
    public static class NonStandardChecker{

        private VariableUpdateRowMapper variableUpdateRowMapper;
        private RDBMSContext rdbmsContext;

        @Before
        public void setup() throws SQLException{
            rdbmsContext = new RDBMSContext();
            DynVar dyn = new DynVar(rdbmsContext);
            rdbmsContext.setInVar(dyn);

            variableUpdateRowMapper = new VariableUpdateRowMapper(rdbmsContext);
        }

        @Test
        public void testBINARY() throws SQLException{
            byte[] bytes = new byte[]{1};

            ResultSet mockResultSet = createMockedResultSet("BINARY", Types.BINARY);
            when(mockResultSet.getBytes(1)).thenReturn(bytes);

            variableUpdateRowMapper.map(mockResultSet);

            DynVar dynVar = rdbmsContext.getOutVar();

            assertNotNull(dynVar);
            assertTrue(dynVar.getFirstClassFromKey(LABEL).isAssignableFrom(bytes.getClass()));

            List values = dynVar.getValues(LABEL);
            assertNotNull(values);
            assertEquals(1, values.size());

            Object value = values.get(0);
            assertEquals(bytes, value);
        }

        @Test
        public void testVARBINARY() throws SQLException{
            byte[] bytes = new byte[]{1};

            ResultSet mockResultSet = createMockedResultSet("VARBINARY", Types.VARBINARY);
            when(mockResultSet.getBytes(1)).thenReturn(bytes);

            variableUpdateRowMapper.map(mockResultSet);

            DynVar dynVar = rdbmsContext.getOutVar();

            assertNotNull(dynVar);
            assertTrue(dynVar.getFirstClassFromKey(LABEL).isAssignableFrom(bytes.getClass()));

            List values = dynVar.getValues(LABEL);
            assertNotNull(values);
            assertEquals(1, values.size());

            Object value = values.get(0);
            assertEquals(bytes, value);
        }

        @Test
        public void testLONGVARBINARY() throws SQLException{
            byte[] bytes = new byte[]{1};

            ResultSet mockResultSet = createMockedResultSet("LONGVARBINARY", Types.LONGVARBINARY);
            when(mockResultSet.getBytes(1)).thenReturn(bytes);

            variableUpdateRowMapper.map(mockResultSet);

            DynVar dynVar = rdbmsContext.getOutVar();

            assertNotNull(dynVar);
            assertTrue(dynVar.getFirstClassFromKey(LABEL).isAssignableFrom(bytes.getClass()));

            List values = dynVar.getValues(LABEL);
            assertNotNull(values);
            assertEquals(1, values.size());

            Object value = values.get(0);
            assertEquals(bytes, value);
        }

        @Test
        public void testNull() throws SQLException{
            ResultSet mockResultSet = createMockedResultSet("NULL", Types.NULL);

            variableUpdateRowMapper.map(mockResultSet);

            DynVar dynVar = rdbmsContext.getOutVar();

            assertNotNull(dynVar);
            assertEquals(NullType.class, dynVar.getFirstClassFromKey(LABEL));

            List values = dynVar.getValues(LABEL);
            assertNotNull(values);
            assertEquals(1, values.size());

            Object value = values.get(0);
            assertNull(value);
        }

        @Test
        public void testBLOB() throws SQLException{
            byte[] bytes = new byte[]{1};

            Blob blob = mock(Blob.class);
            when(blob.length()).thenReturn(new Long(bytes.length));
            when(blob.getBytes(1, bytes.length)).thenReturn(bytes);

            ResultSet mockResultSet = createMockedResultSet("BLOB", Types.BLOB);
            when(mockResultSet.getBlob(1)).thenReturn(blob);

            variableUpdateRowMapper.map(mockResultSet);

            DynVar dynVar = rdbmsContext.getOutVar();

            assertNotNull(dynVar);
            assertTrue(dynVar.getFirstClassFromKey(LABEL).isAssignableFrom(bytes.getClass()));

            List values = dynVar.getValues(LABEL);
            assertNotNull(values);
            assertEquals(1, values.size());

            Object value = values.get(0);
            assertEquals(bytes, value);
        }

        @Test
        public void testCLOB() throws SQLException{
            String clobValue = "fake clob value";

            Clob clob = mock(Clob.class);
            when(clob.length()).thenReturn(new Long(clobValue.length()));
            when(clob.getSubString(1, clobValue.length())).thenReturn(clobValue);

            ResultSet mockResultSet = createMockedResultSet("CLOB", Types.CLOB);
            when(mockResultSet.getClob(1)).thenReturn(clob);

            variableUpdateRowMapper.map(mockResultSet);

            DynVar dynVar = rdbmsContext.getOutVar();

            assertNotNull(dynVar);
            assertEquals(String.class, dynVar.getFirstClassFromKey(LABEL));

            List values = dynVar.getValues(LABEL);
            assertNotNull(values);
            assertEquals(1, values.size());

            Object value = values.get(0);
            assertEquals(clobValue, value);
        }

        /**
         * This could not go in {@link EasyValueChecker} because the class gets
         * changed to a {@link java.util.Date}.
         */
        @Test
        public void testTIMESTAMP_NotNull() throws SQLException{
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());

            ResultSet mockResultSet = createMockedResultSet("TIMESTAMP", Types.TIMESTAMP);
            when(mockResultSet.getTimestamp(1)).thenReturn(timestamp);
            when(mockResultSet.wasNull()).thenReturn(false);

            variableUpdateRowMapper.map(mockResultSet);

            DynVar dynVar = rdbmsContext.getOutVar();

            assertNotNull(dynVar);
            assertEquals(java.util.Date.class, dynVar.getFirstClassFromKey(LABEL));

            List values = dynVar.getValues(LABEL);
            assertNotNull(values);
            assertEquals(1, values.size());

            Object value = values.get(0);
            assertEquals(new java.util.Date(timestamp.getTime()), value);
        }

        /**
         * This could not go in {@link EasyValueChecker} because the class gets
         * changed to a {@link java.util.Date}.
         */
        @Test
        public void testTIMESTAMP_Null() throws SQLException{
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());

            ResultSet mockResultSet = createMockedResultSet("TIMESTAMP", Types.TIMESTAMP);
            when(mockResultSet.getTimestamp(1)).thenReturn(timestamp);
            when(mockResultSet.wasNull()).thenReturn(true);

            variableUpdateRowMapper.map(mockResultSet);

            DynVar dynVar = rdbmsContext.getOutVar();

            assertNotNull(dynVar);
            assertEquals(java.util.Date.class, dynVar.getFirstClassFromKey(LABEL));

            List values = dynVar.getValues(LABEL);
            assertNotNull(values);
            assertEquals(1, values.size());

            Object value = values.get(0);
            assertNull(value);
        }

    }

}
