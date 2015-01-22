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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.redhat.lightblue.common.rdbms.RDBMSDataSourceResolver;
import com.redhat.lightblue.crud.CRUDOperationContext;
import com.redhat.lightblue.eval.FieldAccessRoleEvaluator;
import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.metadata.rdbms.model.InOut;
import com.redhat.lightblue.metadata.rdbms.model.RDBMS;
import com.redhat.lightblue.query.Projection;
import com.redhat.lightblue.query.QueryExpression;
import com.redhat.lightblue.query.Sort;
import com.redhat.lightblue.query.UpdateExpression;
import com.redhat.lightblue.util.JsonUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

/**
 * @param <T> type of object returned in List (resultList)
 */
public class RDBMSContext<T> {
    private Range fromToQueryRange;
    private DataSource dataSource;
    private String dataSourceName;
    private Connection connection;
    private PreparedStatement preparedStatement;
    private Boolean resultBoolean;
    private Integer resultInteger = 0;
    private RowMapper<T> rowMapper;
    private List<T> resultList;
    private RDBMS rdbms;
    private String sql;
    private String type;
    private EntityMetadata entityMetadata;
    private QueryExpression queryExpression;
    private Projection projection;
    private Sort sort;
    private Map<String, Object> temporaryVariable;
    private List<InOut> in = new ArrayList<>();
    private List<InOut> out = new ArrayList<>();
    private DynVar inVar;
    private DynVar outVar;
    private boolean initialInput;
    private HashMap<String, Object> inputMappedByField;
    private HashMap<String, Object> inputMappedByColumn;
    private JsonNodeFactory jsonNodeFactory;
    private com.redhat.lightblue.common.rdbms.RDBMSDataSourceResolver RDBMSDataSourceResolver;
    private FieldAccessRoleEvaluator fieldAccessRoleEvaluator;
    private String CRUDOperationName;
    private CRUDOperationContext crudOperationContext;
    private String currentLoopOperator;
    private UpdateExpression updateExpression;

    public RDBMSContext() {
        this.fromToQueryRange =  new Range();
        inVar = new DynVar(this);
        outVar = new DynVar(this);
    }

    public RDBMSContext(Long from, Long to, QueryExpression queryExpression, Projection projection, EntityMetadata entityMetadata, JsonNodeFactory jsonNodeFactory, com.redhat.lightblue.common.rdbms.RDBMSDataSourceResolver RDBMSDataSourceResolver, FieldAccessRoleEvaluator fieldAccessRoleEvaluator,CRUDOperationContext crudOperationContext, String CRUDOperationName) {
        this();
        this.fromToQueryRange =  new Range(from, to);
        this.queryExpression = queryExpression;
        this.projection = projection;
        this.entityMetadata = entityMetadata;
        this.jsonNodeFactory = jsonNodeFactory;
        this.RDBMSDataSourceResolver = RDBMSDataSourceResolver;
        this.fieldAccessRoleEvaluator = fieldAccessRoleEvaluator;
        this.crudOperationContext = crudOperationContext;
        this.CRUDOperationName = CRUDOperationName;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public String getDataSourceName() {
        return dataSourceName;
    }

    public void setDataSourceName(String dataSourceName) {
        this.dataSourceName = dataSourceName;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public PreparedStatement getPreparedStatement() {
        return preparedStatement;
    }

    public void setPreparedStatement(PreparedStatement preparedStatement) {
        this.preparedStatement = preparedStatement;
    }

    public void setResultBoolean(Boolean resultBoolean) {
        this.resultBoolean = resultBoolean;
    }

    public Integer getResultInteger() {
        return resultInteger;
    }

    public void setResultInteger(Integer resultInteger) {
        this.resultInteger = resultInteger;
    }

    public RowMapper<T> getRowMapper() {
        return rowMapper;
    }

    public void setRowMapper(RowMapper<T> rowMapper) {
        this.rowMapper = rowMapper;
    }

    public List<T> getResultList() {
        return resultList;
    }

    public void setResultList(List<T> resultList) {
        this.resultList = resultList;
    }

    public RDBMS getRdbms() {
        return rdbms;
    }

    public void setRdbms(RDBMS rdbms) {
        this.rdbms = rdbms;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public EntityMetadata getEntityMetadata() {
        return entityMetadata;
    }

    public void setEntityMetadata(EntityMetadata entityMetadata) {
        this.entityMetadata = entityMetadata;
    }

    public QueryExpression getQueryExpression() {
        return queryExpression;
    }

    public void setQueryExpression(QueryExpression queryExpression) {
        this.queryExpression = queryExpression;
    }

    public Projection getProjection() {
        return projection;
    }

    public void setProjection(Projection projection) {
        this.projection = projection;
    }

    public Sort getSort() {
        return sort;
    }

    public void setSort(Sort sort) {
        this.sort = sort;
    }

    public Boolean getResultBoolean() {
        return resultBoolean;
    }

    public Map<String, Object> getTemporaryVariable() {
        return temporaryVariable;
    }

    public void setTemporaryVariable(Map<String, Object> temporaryVariable) {
        this.temporaryVariable = temporaryVariable;
    }

    public List<InOut> getIn() {
        return in;
    }

    public void setIn(List<InOut> in) {
        this.in = in;
    }

    public List<InOut> getOut() {
        return out;
    }

    public void setOut(List<InOut> out) {
        this.out = out;
    }

    public DynVar getInVar() {
        return inVar;
    }

    public void setInVar(DynVar inVar) {
        this.inVar = inVar;
    }

    public DynVar getOutVar() {
        return outVar;
    }

    public void setOutVar(DynVar outVar) {
        this.outVar = outVar;
    }

    public void setInitialInput(boolean initialInput) {
        this.initialInput = initialInput;
    }

    public boolean isInitialInput() {
        return initialInput;
    }

    public void setInputMappedByField(HashMap<String, Object> inputMappedByField) {
        this.inputMappedByField = inputMappedByField;
    }

    public HashMap<String, Object> getInputMappedByField() {
        return inputMappedByField;
    }

    public void setInputMappedByColumn(HashMap<String, Object> inputMappedByColumn) {
        this.inputMappedByColumn = inputMappedByColumn;
    }

    public HashMap<String, Object> getInputMappedByColumn() {
        return inputMappedByColumn;
    }

    public void setJsonNodeFactory(JsonNodeFactory jsonNodeFactory) {
        this.jsonNodeFactory = jsonNodeFactory;
    }

    public JsonNodeFactory getJsonNodeFactory() {
        return jsonNodeFactory;
    }

    public void setRDBMSDataSourceResolver(RDBMSDataSourceResolver RDBMSDataSourceResolver) {
        this.RDBMSDataSourceResolver = RDBMSDataSourceResolver;
    }

    public RDBMSDataSourceResolver getRDBMSDataSourceResolver() {
        return RDBMSDataSourceResolver;
    }

    public void setFieldAccessRoleEvaluator(FieldAccessRoleEvaluator fieldAccessRoleEvaluator) {
        this.fieldAccessRoleEvaluator = fieldAccessRoleEvaluator;
    }

    public FieldAccessRoleEvaluator getFieldAccessRoleEvaluator() {
        return fieldAccessRoleEvaluator;
    }

    public void setCRUDOperationName(String CRUDOperationName) {
        this.CRUDOperationName = CRUDOperationName;
    }

    public String getCRUDOperationName() {
        return CRUDOperationName;
    }

    public void setCrudOperationContext(CRUDOperationContext crudOperationContext) {
        this.crudOperationContext = crudOperationContext;
    }

    public CRUDOperationContext getCrudOperationContext() {
        return crudOperationContext;
    }

    public void setCurrentLoopOperator(String currentLoopOperator) {
        this.currentLoopOperator = currentLoopOperator;
    }

    public String getCurrentLoopOperator() {
        return currentLoopOperator;
    }

    public Range getFromToQueryRange() {
        return fromToQueryRange;
    }

    public void setFromToQueryRange(Range fromToQueryRange) {
        this.fromToQueryRange = fromToQueryRange;
    }

    public void setUpdateExpression(UpdateExpression updateExpression) {
        this.updateExpression = updateExpression;
    }

    public UpdateExpression getUpdateExpression() {
        return updateExpression;
    }

    @Override
    public String toString() {
        JsonNodeFactory jsonNodeFactory1 = jsonNodeFactory;
        if(jsonNodeFactory == null) {
            jsonNodeFactory1 = JsonNodeFactory.withExactBigDecimals(true);
        }
        ObjectNode objectNode = jsonNodeFactory1.objectNode();
        NullNode nullNode = jsonNodeFactory1.nullNode();
        String nullNodeString = JsonUtils.prettyPrint(nullNode);
        String s = null;
        JsonNode j = null;
        try {

            s = fromToQueryRange == null? nullNodeString : fromToQueryRange.toString();
            objectNode.set("fromToQueryRange", JsonUtils.json(s));

            j = dataSource == null? nullNode : jsonNodeFactory1.textNode(dataSource.toString());
            objectNode.set("dataSource", j);

            j = dataSourceName == null? nullNode : jsonNodeFactory1.textNode(dataSourceName.toString());
            objectNode.set("dataSourceName", j);

            j = connection == null? nullNode : jsonNodeFactory1.textNode(connection.toString());
            objectNode.set("connection", j);

            j = preparedStatement == null? nullNode : jsonNodeFactory1.textNode(preparedStatement.toString());
            objectNode.set("preparedStatement", j);

            j = resultBoolean == null? nullNode : jsonNodeFactory1.booleanNode(resultBoolean);
            objectNode.set("resultBoolean", j);

            j = resultInteger == null? nullNode : jsonNodeFactory1.numberNode(resultInteger);
            objectNode.set("resultInteger", j);

            j = rowMapper == null? nullNode : jsonNodeFactory1.textNode(rowMapper.toString());
            objectNode.set("rowMapper", j);

            if(resultList != null) {
                ArrayNode jsonNodes = jsonNodeFactory1.arrayNode();
                for(T a : resultList){
                    jsonNodes.add(a.toString());
                }
                objectNode.set("resultList", jsonNodes);
            } else {
                objectNode.set("resultList", nullNode);
            }

            s = rdbms == null? nullNodeString : rdbms.toString();
            objectNode.set("rdbms", JsonUtils.json(s));

            j = sql == null? nullNode : jsonNodeFactory1.textNode(sql.toString());
            objectNode.set("sql", j);

            j = type == null? nullNode : jsonNodeFactory1.textNode(type.toString());
            objectNode.set("type", j);

            j = entityMetadata == null? nullNode : jsonNodeFactory1.textNode(entityMetadata.toString());
            objectNode.set("entityMetadata", j);

            j = queryExpression == null? nullNode : jsonNodeFactory1.textNode(queryExpression.toString());
            objectNode.set("queryExpression", j);

            j = projection == null? nullNode : jsonNodeFactory1.textNode(projection.toString());
            objectNode.set("projection", j);

            j = sort == null? nullNode : jsonNodeFactory1.textNode(sort.toString());
            objectNode.set("sort", j);

            if(temporaryVariable != null) {
                ObjectNode jsonNodes = jsonNodeFactory1.objectNode();
                for(Map.Entry<String, Object> a : temporaryVariable.entrySet()){
                    jsonNodes.set(a.getKey(),jsonNodeFactory1.textNode(a.getValue().toString()));
                }
                objectNode.set("temporaryVariable", jsonNodes);
            } else {
                objectNode.set("temporaryVariable", nullNode);
            }

            if(in != null) {
                ArrayNode jsonNodes = jsonNodeFactory1.arrayNode();
                for(InOut a : in){
                    jsonNodes.add(a.toString());
                }
                objectNode.set("in", jsonNodes);
            } else {
                objectNode.set("in", nullNode);
            }

            s = out == null? nullNodeString : out.toString();
            if(out != null) {
                ArrayNode jsonNodes = jsonNodeFactory1.arrayNode();
                for(InOut a : out){
                    jsonNodes.add(a.toString());
                }
                objectNode.set("out", jsonNodes);
            } else {
                objectNode.set("out", nullNode);
            }

            s = inVar == null? nullNodeString : inVar.toString();
            objectNode.set("inVar", JsonUtils.json(s));

            s = outVar == null? nullNodeString : outVar.toString();
            objectNode.set("outVar", JsonUtils.json(s));

            objectNode.set("initialInput", JsonUtils.json(Boolean.toString(initialInput)));

            if(inputMappedByField != null) {
                ObjectNode jsonNodes = jsonNodeFactory1.objectNode();
                for(Map.Entry<String, Object> a : inputMappedByField.entrySet()){
                    jsonNodes.set(a.getKey(),jsonNodeFactory1.textNode(a.getValue().toString()));
                }
                objectNode.set("inputMappedByField", jsonNodes);
            } else {
                objectNode.set("inputMappedByField", nullNode);
            }

            if(inputMappedByColumn != null) {
                ObjectNode jsonNodes = jsonNodeFactory1.objectNode();
                for(Map.Entry<String, Object> a : inputMappedByColumn.entrySet()){
                    jsonNodes.set(a.getKey(),jsonNodeFactory1.textNode(a.getValue().toString()));
                }
                objectNode.set("inputMappedByColumn", jsonNodes);
            } else {
                objectNode.set("inputMappedByColumn", nullNode);
            }

            s = jsonNodeFactory == null? nullNodeString : jsonNodeFactory.toString();
            objectNode.set("jsonNodeFactory", jsonNodeFactory1.textNode(s));

            s = RDBMSDataSourceResolver == null? nullNodeString : RDBMSDataSourceResolver.toString();
            objectNode.set("RDBMSDataSourceResolver", JsonUtils.json(s));

            s = fieldAccessRoleEvaluator == null? nullNodeString : fieldAccessRoleEvaluator.toString();
            objectNode.set("fieldAccessRoleEvaluator", jsonNodeFactory1.textNode(s));

            s = CRUDOperationName == null? nullNodeString : CRUDOperationName.toString();
            objectNode.set("CRUDOperationName", jsonNodeFactory1.textNode(s));

            s = crudOperationContext == null? nullNodeString : crudOperationContext.toString();
            objectNode.set("crudOperationContext", jsonNodeFactory1.textNode(s));

            s = currentLoopOperator == null? nullNodeString : currentLoopOperator.toString();
            objectNode.set("currentLoopOperator", jsonNodeFactory1.textNode(s));

            s = updateExpression == null? nullNodeString : updateExpression.toString();
            objectNode.set("updateExpression", jsonNodeFactory1.textNode(s));

        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        return JsonUtils.prettyPrint(objectNode);
    }
}
