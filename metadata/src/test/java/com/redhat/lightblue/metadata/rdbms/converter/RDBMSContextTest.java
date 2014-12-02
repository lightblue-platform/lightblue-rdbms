package com.redhat.lightblue.metadata.rdbms.converter;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.redhat.lightblue.common.rdbms.RDBMSDataSourceResolver;
import com.redhat.lightblue.common.rdbms.RDBMSDataStore;
import com.redhat.lightblue.crud.CRUDOperationContext;
import com.redhat.lightblue.crud.Factory;
import com.redhat.lightblue.crud.Operation;
import com.redhat.lightblue.eval.FieldAccessRoleEvaluator;
import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.metadata.rdbms.model.*;
import com.redhat.lightblue.query.*;
import com.redhat.lightblue.util.JsonDoc;
import com.redhat.lightblue.util.JsonUtils;
import com.redhat.lightblue.util.Path;
import org.hsqldb.jdbc.JDBCConnection;
import org.hsqldb.jdbc.JDBCDataSource;
import org.hsqldb.jdbc.JDBCPreparedStatement;
import org.hsqldb.persist.HsqlProperties;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.Executor;

import static org.junit.Assert.*;

public class RDBMSContextTest {

    @Test
    public void testToString() throws Exception {
        RDBMSContext rdbmsContext = new RDBMSContext();
        String s = rdbmsContext.toString();
        System.out.println(s);
        JsonNode json = JsonUtils.json(s);

        for(Field f : RDBMSContext.class.getDeclaredFields()){
            JsonNode jsonNode = json.get(f.getName());
            assertNotNull("Missing field to map in the RDBMSContext.toString() method",jsonNode);
        }
    }


    @Test
    public void testToStringWithObjects() throws Exception {
        RDBMSContext rdbmsContext = new RDBMSContext();
        rdbmsContext.setCrudOperationContext(new MyCRUDOperationContext());
        rdbmsContext.setType("Type");
        rdbmsContext.setProjection(new FieldProjection(new Path(""), false, false));
        rdbmsContext.setSql("SQL");
        rdbmsContext.setDataSource(new JDBCDataSource());
        rdbmsContext.setConnection(new MyConnection());
        rdbmsContext.setCurrentLoopOperator("CurrentLoopOperator");
        rdbmsContext.setCRUDOperationName("CRUDOperationName");
        rdbmsContext.setDataSourceName("DataSourceName");
        rdbmsContext.setEntityMetadata(new EntityMetadata("Name"));
        rdbmsContext.setFieldAccessRoleEvaluator(new FieldAccessRoleEvaluator(new EntityMetadata("Name"), new HashSet<String>()));
        rdbmsContext.setFromToQueryRange(new Range(1L,2L));
        ArrayList<InOut> in = new ArrayList<InOut>();
        in.add(new InOut());
        rdbmsContext.setIn(in);
        rdbmsContext.setInitialInput(false);
        rdbmsContext.setUpdateExpression(new SetExpression(UpdateOperator._set, new ArrayList<FieldAndRValue>() ));
        rdbmsContext.setTemporaryVariable(new HashMap<String, Object>());
        rdbmsContext.setSort(new SortKey(new Path(""), false));
        rdbmsContext.setRowMapper(new MyRowMapper());
        rdbmsContext.setResultList(new ArrayList());
        rdbmsContext.setResultInteger(1);
        rdbmsContext.setResultBoolean(false);
        rdbmsContext.setRDBMSDataSourceResolver(new MyRDBMSDataSourceResolver());
        RDBMS rdbms = new RDBMS();
        com.redhat.lightblue.metadata.rdbms.model.Operation save = new com.redhat.lightblue.metadata.rdbms.model.Operation();
        save.setName("save");
        save.setExpressionList(new ArrayList<Expression>());
        com.redhat.lightblue.metadata.rdbms.model.Statement statement = new com.redhat.lightblue.metadata.rdbms.model.Statement();
        statement.setSQL("update SQL");
        statement.setType("update");
        save.getExpressionList().add(statement);
        rdbms.setSave(save);
        rdbms.setDialect("oracle");
        rdbmsContext.setRdbms(rdbms);
        rdbmsContext.setQueryExpression(new FieldComparisonExpression(new Path("P1"),BinaryComparisonOperator._eq, new Path("P2")));
        rdbmsContext.setPreparedStatement(new MyPreparedStatement());
        rdbmsContext.setInputMappedByColumn(new HashMap<String, Object>());
        rdbmsContext.setInputMappedByField(new HashMap<String, Object>());

        String s = rdbmsContext.toString();
        System.out.println(s);
        JsonNode json = JsonUtils.json(s);

        for(Field f : RDBMSContext.class.getDeclaredFields()){
            JsonNode jsonNode = json.get(f.getName());
            assertNotNull("Missing field to map in the RDBMSContext.toString() method",jsonNode);
        }
    }

    private static class MyCRUDOperationContext extends CRUDOperationContext {
        public MyCRUDOperationContext() {
            this(Operation.SAVE, "entityName", new Factory(), new HashSet<String>(),new ArrayList<JsonDoc>());
        }

        public MyCRUDOperationContext(Operation op, String entityName, Factory f, Set<String> callerRoles, List<JsonDoc> docs) {
            super(op, entityName, f, callerRoles, docs);
        }

        @Override
        public EntityMetadata getEntityMetadata(String s) {
            return null;
        }
    }

    private static class MyConnection implements Connection {
        @Override
        public Statement createStatement() throws SQLException {
            return null;
        }

        @Override
        public PreparedStatement prepareStatement(String s) throws SQLException {
            return null;
        }

        @Override
        public CallableStatement prepareCall(String s) throws SQLException {
            return null;
        }

        @Override
        public String nativeSQL(String s) throws SQLException {
            return null;
        }

        @Override
        public void setAutoCommit(boolean b) throws SQLException {

        }

        @Override
        public boolean getAutoCommit() throws SQLException {
            return false;
        }

        @Override
        public void commit() throws SQLException {

        }

        @Override
        public void rollback() throws SQLException {

        }

        @Override
        public void close() throws SQLException {

        }

        @Override
        public boolean isClosed() throws SQLException {
            return false;
        }

        @Override
        public DatabaseMetaData getMetaData() throws SQLException {
            return null;
        }

        @Override
        public void setReadOnly(boolean b) throws SQLException {

        }

        @Override
        public boolean isReadOnly() throws SQLException {
            return false;
        }

        @Override
        public void setCatalog(String s) throws SQLException {

        }

        @Override
        public String getCatalog() throws SQLException {
            return null;
        }

        @Override
        public void setTransactionIsolation(int i) throws SQLException {

        }

        @Override
        public int getTransactionIsolation() throws SQLException {
            return 0;
        }

        @Override
        public SQLWarning getWarnings() throws SQLException {
            return null;
        }

        @Override
        public void clearWarnings() throws SQLException {

        }

        @Override
        public Statement createStatement(int i, int i2) throws SQLException {
            return null;
        }

        @Override
        public PreparedStatement prepareStatement(String s, int i, int i2) throws SQLException {
            return null;
        }

        @Override
        public CallableStatement prepareCall(String s, int i, int i2) throws SQLException {
            return null;
        }

        @Override
        public Map<String, Class<?>> getTypeMap() throws SQLException {
            return null;
        }

        @Override
        public void setTypeMap(Map<String, Class<?>> stringClassMap) throws SQLException {

        }

        @Override
        public void setHoldability(int i) throws SQLException {

        }

        @Override
        public int getHoldability() throws SQLException {
            return 0;
        }

        @Override
        public Savepoint setSavepoint() throws SQLException {
            return null;
        }

        @Override
        public Savepoint setSavepoint(String s) throws SQLException {
            return null;
        }

        @Override
        public void rollback(Savepoint savepoint) throws SQLException {

        }

        @Override
        public void releaseSavepoint(Savepoint savepoint) throws SQLException {

        }

        @Override
        public Statement createStatement(int i, int i2, int i3) throws SQLException {
            return null;
        }

        @Override
        public PreparedStatement prepareStatement(String s, int i, int i2, int i3) throws SQLException {
            return null;
        }

        @Override
        public CallableStatement prepareCall(String s, int i, int i2, int i3) throws SQLException {
            return null;
        }

        @Override
        public PreparedStatement prepareStatement(String s, int i) throws SQLException {
            return null;
        }

        @Override
        public PreparedStatement prepareStatement(String s, int[] ints) throws SQLException {
            return null;
        }

        @Override
        public PreparedStatement prepareStatement(String s, String[] strings) throws SQLException {
            return null;
        }

        @Override
        public Clob createClob() throws SQLException {
            return null;
        }

        @Override
        public Blob createBlob() throws SQLException {
            return null;
        }

        @Override
        public NClob createNClob() throws SQLException {
            return null;
        }

        @Override
        public SQLXML createSQLXML() throws SQLException {
            return null;
        }

        @Override
        public boolean isValid(int i) throws SQLException {
            return false;
        }

        @Override
        public void setClientInfo(String s, String s2) throws SQLClientInfoException {

        }

        @Override
        public void setClientInfo(Properties properties) throws SQLClientInfoException {

        }

        @Override
        public String getClientInfo(String s) throws SQLException {
            return null;
        }

        @Override
        public Properties getClientInfo() throws SQLException {
            return null;
        }

        @Override
        public Array createArrayOf(String s, Object[] objects) throws SQLException {
            return null;
        }

        @Override
        public Struct createStruct(String s, Object[] objects) throws SQLException {
            return null;
        }

        @Override
        public void setSchema(String s) throws SQLException {

        }

        @Override
        public String getSchema() throws SQLException {
            return null;
        }

        @Override
        public void abort(Executor executor) throws SQLException {

        }

        @Override
        public void setNetworkTimeout(Executor executor, int i) throws SQLException {

        }

        @Override
        public int getNetworkTimeout() throws SQLException {
            return 0;
        }

        @Override
        public <T> T unwrap(Class<T> tClass) throws SQLException {
            return null;
        }

        @Override
        public boolean isWrapperFor(Class<?> aClass) throws SQLException {
            return false;
        }
    }

    private static class MyRowMapper implements RowMapper {
        @Override
        public Object map(ResultSet resultSet) throws SQLException {
            return null;
        }
    }

    private static class MyPreparedStatement implements PreparedStatement {
        @Override
        public ResultSet executeQuery() throws SQLException {
            return null;
        }

        @Override
        public int executeUpdate() throws SQLException {
            return 0;
        }

        @Override
        public void setNull(int i, int i2) throws SQLException {

        }

        @Override
        public void setBoolean(int i, boolean b) throws SQLException {

        }

        @Override
        public void setByte(int i, byte b) throws SQLException {

        }

        @Override
        public void setShort(int i, short i2) throws SQLException {

        }

        @Override
        public void setInt(int i, int i2) throws SQLException {

        }

        @Override
        public void setLong(int i, long l) throws SQLException {

        }

        @Override
        public void setFloat(int i, float v) throws SQLException {

        }

        @Override
        public void setDouble(int i, double v) throws SQLException {

        }

        @Override
        public void setBigDecimal(int i, BigDecimal bigDecimal) throws SQLException {

        }

        @Override
        public void setString(int i, String s) throws SQLException {

        }

        @Override
        public void setBytes(int i, byte[] bytes) throws SQLException {

        }

        @Override
        public void setDate(int i, Date date) throws SQLException {

        }

        @Override
        public void setTime(int i, Time time) throws SQLException {

        }

        @Override
        public void setTimestamp(int i, Timestamp timestamp) throws SQLException {

        }

        @Override
        public void setAsciiStream(int i, InputStream inputStream, int i2) throws SQLException {

        }

        @Override
        public void setUnicodeStream(int i, InputStream inputStream, int i2) throws SQLException {

        }

        @Override
        public void setBinaryStream(int i, InputStream inputStream, int i2) throws SQLException {

        }

        @Override
        public void clearParameters() throws SQLException {

        }

        @Override
        public void setObject(int i, Object o, int i2) throws SQLException {

        }

        @Override
        public void setObject(int i, Object o) throws SQLException {

        }

        @Override
        public boolean execute() throws SQLException {
            return false;
        }

        @Override
        public void addBatch() throws SQLException {

        }

        @Override
        public void setCharacterStream(int i, Reader reader, int i2) throws SQLException {

        }

        @Override
        public void setRef(int i, Ref ref) throws SQLException {

        }

        @Override
        public void setBlob(int i, Blob blob) throws SQLException {

        }

        @Override
        public void setClob(int i, Clob clob) throws SQLException {

        }

        @Override
        public void setArray(int i, Array array) throws SQLException {

        }

        @Override
        public ResultSetMetaData getMetaData() throws SQLException {
            return null;
        }

        @Override
        public void setDate(int i, Date date, Calendar calendar) throws SQLException {

        }

        @Override
        public void setTime(int i, Time time, Calendar calendar) throws SQLException {

        }

        @Override
        public void setTimestamp(int i, Timestamp timestamp, Calendar calendar) throws SQLException {

        }

        @Override
        public void setNull(int i, int i2, String s) throws SQLException {

        }

        @Override
        public void setURL(int i, URL url) throws SQLException {

        }

        @Override
        public ParameterMetaData getParameterMetaData() throws SQLException {
            return null;
        }

        @Override
        public void setRowId(int i, RowId rowId) throws SQLException {

        }

        @Override
        public void setNString(int i, String s) throws SQLException {

        }

        @Override
        public void setNCharacterStream(int i, Reader reader, long l) throws SQLException {

        }

        @Override
        public void setNClob(int i, NClob nClob) throws SQLException {

        }

        @Override
        public void setClob(int i, Reader reader, long l) throws SQLException {

        }

        @Override
        public void setBlob(int i, InputStream inputStream, long l) throws SQLException {

        }

        @Override
        public void setNClob(int i, Reader reader, long l) throws SQLException {

        }

        @Override
        public void setSQLXML(int i, SQLXML sqlxml) throws SQLException {

        }

        @Override
        public void setObject(int i, Object o, int i2, int i3) throws SQLException {

        }

        @Override
        public void setAsciiStream(int i, InputStream inputStream, long l) throws SQLException {

        }

        @Override
        public void setBinaryStream(int i, InputStream inputStream, long l) throws SQLException {

        }

        @Override
        public void setCharacterStream(int i, Reader reader, long l) throws SQLException {

        }

        @Override
        public void setAsciiStream(int i, InputStream inputStream) throws SQLException {

        }

        @Override
        public void setBinaryStream(int i, InputStream inputStream) throws SQLException {

        }

        @Override
        public void setCharacterStream(int i, Reader reader) throws SQLException {

        }

        @Override
        public void setNCharacterStream(int i, Reader reader) throws SQLException {

        }

        @Override
        public void setClob(int i, Reader reader) throws SQLException {

        }

        @Override
        public void setBlob(int i, InputStream inputStream) throws SQLException {

        }

        @Override
        public void setNClob(int i, Reader reader) throws SQLException {

        }

        @Override
        public ResultSet executeQuery(String s) throws SQLException {
            return null;
        }

        @Override
        public int executeUpdate(String s) throws SQLException {
            return 0;
        }

        @Override
        public void close() throws SQLException {

        }

        @Override
        public int getMaxFieldSize() throws SQLException {
            return 0;
        }

        @Override
        public void setMaxFieldSize(int i) throws SQLException {

        }

        @Override
        public int getMaxRows() throws SQLException {
            return 0;
        }

        @Override
        public void setMaxRows(int i) throws SQLException {

        }

        @Override
        public void setEscapeProcessing(boolean b) throws SQLException {

        }

        @Override
        public int getQueryTimeout() throws SQLException {
            return 0;
        }

        @Override
        public void setQueryTimeout(int i) throws SQLException {

        }

        @Override
        public void cancel() throws SQLException {

        }

        @Override
        public SQLWarning getWarnings() throws SQLException {
            return null;
        }

        @Override
        public void clearWarnings() throws SQLException {

        }

        @Override
        public void setCursorName(String s) throws SQLException {

        }

        @Override
        public boolean execute(String s) throws SQLException {
            return false;
        }

        @Override
        public ResultSet getResultSet() throws SQLException {
            return null;
        }

        @Override
        public int getUpdateCount() throws SQLException {
            return 0;
        }

        @Override
        public boolean getMoreResults() throws SQLException {
            return false;
        }

        @Override
        public void setFetchDirection(int i) throws SQLException {

        }

        @Override
        public int getFetchDirection() throws SQLException {
            return 0;
        }

        @Override
        public void setFetchSize(int i) throws SQLException {

        }

        @Override
        public int getFetchSize() throws SQLException {
            return 0;
        }

        @Override
        public int getResultSetConcurrency() throws SQLException {
            return 0;
        }

        @Override
        public int getResultSetType() throws SQLException {
            return 0;
        }

        @Override
        public void addBatch(String s) throws SQLException {

        }

        @Override
        public void clearBatch() throws SQLException {

        }

        @Override
        public int[] executeBatch() throws SQLException {
            return new int[0];
        }

        @Override
        public Connection getConnection() throws SQLException {
            return null;
        }

        @Override
        public boolean getMoreResults(int i) throws SQLException {
            return false;
        }

        @Override
        public ResultSet getGeneratedKeys() throws SQLException {
            return null;
        }

        @Override
        public int executeUpdate(String s, int i) throws SQLException {
            return 0;
        }

        @Override
        public int executeUpdate(String s, int[] ints) throws SQLException {
            return 0;
        }

        @Override
        public int executeUpdate(String s, String[] strings) throws SQLException {
            return 0;
        }

        @Override
        public boolean execute(String s, int i) throws SQLException {
            return false;
        }

        @Override
        public boolean execute(String s, int[] ints) throws SQLException {
            return false;
        }

        @Override
        public boolean execute(String s, String[] strings) throws SQLException {
            return false;
        }

        @Override
        public int getResultSetHoldability() throws SQLException {
            return 0;
        }

        @Override
        public boolean isClosed() throws SQLException {
            return false;
        }

        @Override
        public void setPoolable(boolean b) throws SQLException {

        }

        @Override
        public boolean isPoolable() throws SQLException {
            return false;
        }

        @Override
        public void closeOnCompletion() throws SQLException {

        }

        @Override
        public boolean isCloseOnCompletion() throws SQLException {
            return false;
        }

        @Override
        public <T> T unwrap(Class<T> tClass) throws SQLException {
            return null;
        }

        @Override
        public boolean isWrapperFor(Class<?> aClass) throws SQLException {
            return false;
        }
    }

    private static class MyRDBMSDataSourceResolver implements RDBMSDataSourceResolver {
        @Override public DataSource get(RDBMSDataStore store) {
            return null;
        }

        @Override  public String toString() {
            return "{}";//JsonNodeFactory.withExactBigDecimals(true).objectNode().toString();
        }
    }
}