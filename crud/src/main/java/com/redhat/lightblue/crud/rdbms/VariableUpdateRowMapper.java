/*
 Copyright 2013 Red Hat, Inc. and/or its affiliates.

 This file is part of lightblue.

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.redhat.lightblue.crud.rdbms;

import com.redhat.lightblue.metadata.rdbms.converter.DynVar;
import com.redhat.lightblue.metadata.rdbms.converter.RowMapper;
import com.redhat.lightblue.metadata.rdbms.converter.RDBMSContext;
import com.redhat.lightblue.metadata.rdbms.util.Column;

import javax.lang.model.type.NullType;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Maps a row into DynVar
 *
 * @author lcestari
 */
public class VariableUpdateRowMapper<Void> implements RowMapper<Void> {
    private RDBMSContext rdbmsContext;

    public VariableUpdateRowMapper(RDBMSContext rdbmsContext) {
        this.rdbmsContext = rdbmsContext;
    }

    @Override
    public Void map(ResultSet rs) throws SQLException {
        List<Column> colList = new ArrayList<>();
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i < columnCount + 1; i++) {
            int position = i;
            String name = rsmd.getColumnName(i);
            String alias = rsmd.getColumnLabel(i);
            String clazz = rsmd.getColumnClassName(i);
            int type = rsmd.getColumnType(i);
            colList.add(new Column(i,name,alias,clazz,type));
        }
        DynVar dyn;
        if(rdbmsContext.isInitialInput()){
            dyn = rdbmsContext.getInVar();
        } else {
            dyn = rdbmsContext.getOutVar();
        }

        for (Column column : colList) {
            switch (column.getType()){
                case Types.BIT:
                case Types.BOOLEAN:
                    Boolean b = rs.getBoolean(column.getPosition());
                    if (rs.wasNull()) {
                        b = null;
                    }
                    dyn.put(b, Boolean.class, column);
                    break;

                case Types.TINYINT:
                case Types.SMALLINT:
                    Short s = rs.getShort(column.getPosition());
                    if (rs.wasNull()) {
                        s = null;
                    }
                    dyn.put(s, Short.class, column);
                    break;

                case Types.INTEGER:
                    Integer i = rs.getInt(column.getPosition());
                    if (rs.wasNull()) {
                        i = null;
                    }
                    dyn.put(i, Integer.class, column);
                    break;

                case Types.BIGINT:
                case Types.ROWID:
                    Long l = rs.getLong(column.getPosition());
                    if (rs.wasNull()) {
                        l = null;
                    }
                    dyn.put(l, Long.class, column);
                    break;

                case Types.FLOAT:
                case Types.REAL:
                case Types.DOUBLE:
                    Double d = rs.getDouble(column.getPosition());
                    if (rs.wasNull()) {
                        d = null;
                    }
                    dyn.put(d, Double.class, column);
                    break;

                case Types.NUMERIC:
                case Types.DECIMAL:
                    BigDecimal bd = rs.getBigDecimal(column.getPosition());
                    if (rs.wasNull()) {
                        bd = null;
                    }
                    dyn.put(bd, BigDecimal.class, column);
                    break;

                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                    String st = rs.getString(column.getPosition());
                    if (rs.wasNull()) {
                        st = null;
                    }
                    dyn.put(st, String.class, column);
                    break;

                case Types.DATE:
                    Date date1 = rs.getDate(column.getPosition());
                    java.util.Date date = null;
                    if (rs.wasNull()) {
                        date1 = null;
                    }
                    if (date1 != null){
                        date = new java.util.Date(date1.getTime());
                    }
                    dyn.put(date, java.util.Date.class, column);
                    break;

                case Types.TIME:
                    Time time = rs.getTime(column.getPosition());
                    if (rs.wasNull()) {
                        time = null;
                    }
                    dyn.put(time, Time.class, column);
                    break;

                case Types.TIMESTAMP:
                    Timestamp timestamp = rs.getTimestamp(column.getPosition());
                    java.util.Date dt = null;
                    if (rs.wasNull()) {
                        timestamp = null;
                    }
                    if (timestamp != null){
                        dt = new java.util.Date(timestamp.getTime());
                    }
                    dyn.put(dt, java.util.Date.class, column);
                    break;

                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    byte[] bytes = rs.getBytes(column.getPosition());
                    dyn.put(bytes, bytes.getClass(), column);
                    break;

                case Types.NULL:
                    Object objNull = null;
                    dyn.put(objNull, NullType.class, column);
                    break;

                case Types.OTHER:
                    throw new UnsupportedOperationException("No implementation for SQL type ARRAY");

                case Types.JAVA_OBJECT:
                    Object object = rs.getObject(column.getPosition());
                    if (rs.wasNull()) {
                        object = null;
                    }
                    dyn.put(object, object.getClass(), column);
                    break;

                case Types.DISTINCT:
                    throw new UnsupportedOperationException("No implementation for SQL type DISTINCT");
                case Types.STRUCT:
                    throw new UnsupportedOperationException("No implementation for SQL type STRUCT");
                case Types.ARRAY:
                    throw new UnsupportedOperationException("No implementation for SQL type ARRAY");

                case Types.BLOB:
                    Blob blob = rs.getBlob(column.getPosition());
                    int blobLength = (int) blob.length();
                    byte[] blobAsBytes = blob.getBytes(1, blobLength);
                    dyn.put(blobAsBytes, blobAsBytes.getClass(), column);
                    blob.free();
                    break;

                case Types.CLOB:
                    Clob clob = rs.getClob(column.getPosition());
                    String clobString = clob.getSubString(1, (int) clob.length());
                    dyn.put(clobString, String.class, column);
                    clob.free();
                    break;

                case Types.REF:
                    throw new UnsupportedOperationException("No implementation for SQL type REF");
                case Types.DATALINK:
                    throw new UnsupportedOperationException("No implementation for SQL type DATALINK");
                case Types.NCLOB:
                    throw new UnsupportedOperationException("No implementation for SQL type NCLOB");
                case Types.SQLXML:
                    throw new UnsupportedOperationException("No implementation for SQL type SQLXML");
                default:
                    throw new UnsupportedOperationException("No implementation for SQL int " + column.getType());
            }
        }

        return null;
    }

}
