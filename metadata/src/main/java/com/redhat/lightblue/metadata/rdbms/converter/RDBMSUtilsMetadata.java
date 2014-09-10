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
package com.redhat.lightblue.metadata.rdbms.converter;

import com.redhat.lightblue.common.rdbms.RDBMSConstants;
import com.redhat.lightblue.util.Error;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class RDBMSUtilsMetadata {
    private static final Logger LOGGER = LoggerFactory.getLogger(RDBMSUtilsMetadata.class);

    public static DataSource getDataSource(RDBMSContext rDBMSContext) {
        if(rDBMSContext.getDataSource() != null){
            return rDBMSContext.getDataSource();
        }
        if(rDBMSContext.getDataSourceName() != null && !rDBMSContext.getDataSourceName().isEmpty()) {
            DataSource dataSource = com.redhat.lightblue.common.rdbms.RDBMSUtils.getDataSource(rDBMSContext.getDataSourceName());
            rDBMSContext.setDataSource(dataSource);
            return dataSource;
        }
        throw new IllegalStateException("No datasource informed!");
    }

    public static Connection getConnection(RDBMSContext context) {
        if (context.getDataSource() == null) {
            throw new IllegalArgumentException("No dataSource supplied");
        }
        LOGGER.debug("getConnection() start");
        Error.push("RDBMSUtils");
        Error.push("getConnection");
        Connection c = null;
        try {
            c = context.getDataSource().getConnection();
        } catch (SQLException e) {
            // throw new Error (preserves current error context)
            LOGGER.error(e.getMessage(), e);
            throw Error.get(RDBMSConstants.ERR_GET_CONNECTION_FAILED, e.getMessage());
        } finally {
            Error.pop();
            Error.pop();
        }
        context.setConnection(c);
        LOGGER.debug("getConnection() stop");
        return c;
    }

    public static PreparedStatement getPreparedStatement(RDBMSContext context) {
        if (context.getConnection() == null) {
            throw new IllegalArgumentException("No connection supplied");
        }
        if (context.getSql() == null) {
            throw new IllegalArgumentException("No sql statement supplied");
        }
        if (context.getType() == null) {
            throw new IllegalArgumentException("No sql statement type supplied");
        }
        LOGGER.debug("getPreparedStatement() start");
        Error.push("RDBMSUtils");
        Error.push("getStatement");
        PreparedStatement ps = null;
        try {
            ps = context.getConnection().prepareStatement(context.getSql());
        } catch (SQLException e) {
            // throw new Error (preserves current error context)
            LOGGER.error(e.getMessage(), e);
            throw Error.get(RDBMSConstants.ERR_GET_STATEMENT_FAILED, e.getMessage());
        } finally {
            Error.pop();
            Error.pop();
        }
        context.setPreparedStatement(ps);
        LOGGER.debug("getPreparedStatement() stop");
        return ps;
    }

    public static PreparedStatement getStatement(RDBMSContext context) {
        if (context.getConnection() == null) {
            throw new IllegalArgumentException("No connection supplied");
        }
        if (context.getSql() == null) {
            throw new IllegalArgumentException("No sql statement supplied");
        }
        if (context.getType() == null) {
            throw new IllegalArgumentException("No sql statement type supplied");
        }
        LOGGER.debug("getStatement() start");
        Error.push("RDBMSUtils");
        Error.push("getStatement");
        PreparedStatement ps = null;
        try {
            NamedParameterStatement nps = new NamedParameterStatement(context.getConnection(), context.getSql());
            DynVar dynVar = context.getInVar();
            processDynVar(context,nps,dynVar);
            dynVar = context.getOutVar();
            processDynVar(context,nps,dynVar);
            ps = nps.getPrepareStatement();
        } catch (SQLException e) {
            // throw new Error (preserves current error context)
            LOGGER.error(e.getMessage(), e);
            throw Error.get(RDBMSConstants.ERR_GET_STATEMENT_FAILED, e.getMessage());
        } finally {
            Error.pop();
            Error.pop();
        }
        context.setPreparedStatement(ps);
        LOGGER.debug("getStatement() stop");
        return ps;
    }
    enum Classes {
        Boolean,Short,Integer,Long,Double,String,Date,Time ;

    }
    public static void processDynVar(RDBMSContext context,NamedParameterStatement nps,DynVar dynVar) {
        //only supports one non-null value now
        try {
            for(String key : dynVar.getKeys()){
                List values = dynVar.getValues(key);
                if(values != null && values.size() > 0){
                    Object o = values.get(0);
                    if(o == null){
                        continue;
                    }
                    Class clazz = dynVar.getFirstClassFromKey(key);
                    Classes z = Classes.valueOf(clazz.getSimpleName());
                    switch (z) {
                        case Boolean:
                            nps.setBoolean(key, (Boolean) o);
                            break;
                        case Short:
                            nps.setInt(key, (Short) o);
                            break;
                        case Integer:
                            nps.setInt(key, (Integer) o);
                            break;
                        case Long:
                            nps.setLong(key, (Long)o);
                            break;
                        case Double:
                            nps.setDouble(key, (Double) o);
                            break;
                        case String:
                            nps.setString(key, o.toString());
                            break;
                        case Date:
                            nps.setTimestamp(key, new Timestamp(((Date)o).getTime()));
                            break;
                        case Time:
                            nps.setTime(key, (Time) o);
                            break;
                        default:
                            throw new IllegalStateException("State not implemented! clazz:"+clazz+" z:"+z+" clazz.getSimpleName():"+clazz.getSimpleName());
                    }
                    if("byte[]".equals(clazz.getSimpleName())){
                        nps.setBytes(key, (byte[]) o);
                    }
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void close(RDBMSContext context) {
        if (context.getConnection() != null) {
            try {
                context.getConnection().close();
            } catch (SQLException e) {
                LOGGER.error(e.getMessage(), e);
            } finally {
                if (context.getPreparedStatement() != null) {
                    try {
                        context.getPreparedStatement().close();
                    } catch (SQLException e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                }
            }
        }
    }

    public static <T> List<T> buildAllMappedList(RDBMSContext<T> context) {
        if (context.getRowMapper() == null) {
            throw new IllegalArgumentException("No rowMapper supplied");
        }
        Error.push("buildMappedList");
        getDataSource(context);
        getConnection(context);
        getStatement(context);
        if (context.getPreparedStatement() == null) {
            throw new IllegalArgumentException("No statement supplied");
        }
        List<T> list = new ArrayList<>();
        context.setResultList(list);

        ResultSet rs;
        try {
            context.getPreparedStatement().execute();
            rs = context.getPreparedStatement().getResultSet();
            if (rs != null) {
                while (rs.next()) {
                    T o = context.getRowMapper().map(rs);
                    list.add(o);
                }
                while (context.getPreparedStatement().getMoreResults()) {
                    //getMoreResults():  implicitly closes any current ResultSet object
                    while (rs.next()) {
                        T o = context.getRowMapper().map(rs);
                        list.add(o);
                    }
                }
            }
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new IllegalStateException(ex);
        }

        Error.pop();
        close(context);
        return list;
    }

    public static <T> List<T> buildAllMappedList(RDBMSContext<T> context, List<SelectStmt> inputStmt) {
        if (inputStmt == null || inputStmt.isEmpty()) {
            throw new IllegalArgumentException("No statement supplied");
        }
        if (context.getRowMapper() == null) {
            throw new IllegalArgumentException("No rowMapper supplied");
        }
        if (inputStmt == null) {
            throw new IllegalArgumentException("No inputStmt supplied");
        }
        Error.push("buildMappedList");
        getDataSource(context);
        getConnection(context);

        List<T> list = new ArrayList<>();
        context.setResultList(list);
        ResultSet rs;
        for (SelectStmt s : inputStmt){
            try {
                PreparedStatement preparedStatement = context.getConnection().prepareStatement(s.generateStatement());
                context.setPreparedStatement(preparedStatement);

                context.getPreparedStatement().execute();
                rs = context.getPreparedStatement().getResultSet();
                if (rs != null) {
                    while (rs.next()) {
                        T o = context.getRowMapper().map(rs);
                        list.add(o);
                    }
                    while (context.getPreparedStatement().getMoreResults()) {
                        //getMoreResults():  implicitly closes any current ResultSet object
                        while (rs.next()) {
                            T o = context.getRowMapper().map(rs);
                            list.add(o);
                        }
                    }
                }
            } catch (SQLException e) {
                LOGGER.error(e.getMessage(), e);
                throw new IllegalStateException(e);
            }
        }

        Error.pop();
        close(context);
        return list;
    }
}
