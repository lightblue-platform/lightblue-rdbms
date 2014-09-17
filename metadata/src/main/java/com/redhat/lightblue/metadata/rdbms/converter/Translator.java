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
import com.redhat.lightblue.crud.CRUDOperationContext;
import com.redhat.lightblue.metadata.*;
import com.redhat.lightblue.metadata.rdbms.enums.LightblueOperators;
import com.redhat.lightblue.metadata.rdbms.model.*;
import com.redhat.lightblue.query.*;
import com.redhat.lightblue.util.*;

import java.util.*;

import com.redhat.lightblue.util.Error;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author lcestari
 */
// TODO Need to define some details how complex queries will be handles, for example: which expression would produce a query which joins two tables with 1->N relationship with paging (limit, offfset and sort), how would needs will be mapped by rdbms' json schema (it would need to map PK (or PKS in case of compose) and know which ones it would need to do a query before (to not brute force and do for all tables)) (in other words the example could be expressed as "find the first X customer after Y and get its first address", where customer 1 -> N addresses)
public abstract class Translator {

    public static Translator ORACLE = new OracleTranslator();
    private static final Logger LOGGER = LoggerFactory.getLogger(Translator.class);
    private static final Map<BinaryComparisonOperator, String> BINARY_TO_SQL = new HashMap<>();
    private static final Map<BinaryComparisonOperator, String> NOTBINARY_TO_SQL = new HashMap<>();
    private static final HashMap<NaryLogicalOperator, String> NARY_TO_SQL = new HashMap<>();
    static {
        BINARY_TO_SQL.put(BinaryComparisonOperator._eq, "=");
        BINARY_TO_SQL.put(BinaryComparisonOperator._neq, "<>");
        BINARY_TO_SQL.put(BinaryComparisonOperator._lt, "<");
        BINARY_TO_SQL.put(BinaryComparisonOperator._gt, ">");
        BINARY_TO_SQL.put(BinaryComparisonOperator._lte, "<=");
        BINARY_TO_SQL.put(BinaryComparisonOperator._gte, ">=");

        NOTBINARY_TO_SQL.put(BinaryComparisonOperator._eq, "<>");
        NOTBINARY_TO_SQL.put(BinaryComparisonOperator._neq, "=");
        NOTBINARY_TO_SQL.put(BinaryComparisonOperator._lt, ">");
        NOTBINARY_TO_SQL.put(BinaryComparisonOperator._gt, "<");
        NOTBINARY_TO_SQL.put(BinaryComparisonOperator._lte, ">=");
        NOTBINARY_TO_SQL.put(BinaryComparisonOperator._gte, "<=");

        NARY_TO_SQL.put(NaryLogicalOperator._and, "and");
        NARY_TO_SQL.put(NaryLogicalOperator._or, "or");
    }

    public String generateStatement(SelectStmt selectStmt) {
        StringBuilder queryStringBuilder = new StringBuilder();
        generatePre(selectStmt, queryStringBuilder);
        generateResultColumns(selectStmt, queryStringBuilder, selectStmt.getResultColumns());
        generateFrom(selectStmt, queryStringBuilder, selectStmt.getFromTables());
        generateWhere(selectStmt, queryStringBuilder, selectStmt.getWhereConditionals());
        generateGroupBy(selectStmt, queryStringBuilder, selectStmt.getGroupBy());
        generateOrderBy(selectStmt, queryStringBuilder, selectStmt.getOrderBy());
        generateLimitOffset(selectStmt, queryStringBuilder, selectStmt.getRange());
        generatePos(selectStmt, queryStringBuilder);

        return queryStringBuilder.toString();
    }

    protected void generatePre(SelectStmt selectStmt, StringBuilder queryStringBuilder) {
        queryStringBuilder.append("SELECT ");
        if(selectStmt.getDistinct()){
            queryStringBuilder.append("DISTINCT ");
        }
    }

    protected void generateResultColumns(SelectStmt selectStmt, StringBuilder queryStringBuilder, List<String> resultColumns) {
        for (String resultColumn : resultColumns) {
            queryStringBuilder.append(resultColumn).append(" ,");
        }
        queryStringBuilder.deleteCharAt(queryStringBuilder.length() - 1); //remove the last ','
    }

    protected void generateFrom(SelectStmt selectStmt, StringBuilder queryStringBuilder, List<String> fromTables) {
        queryStringBuilder.append("FROM ");
        for (String table : fromTables) {
            queryStringBuilder.append(table).append(" ,");
        }
        queryStringBuilder.deleteCharAt(queryStringBuilder.length()-1); //remove the last ','
    }

    protected void generateWhere(SelectStmt selectStmt, StringBuilder queryStringBuilder, LinkedList<String> whereConditionals) {
        queryStringBuilder.append("WHERE ");
        for (String where : whereConditionals) {
            queryStringBuilder.append(where).append(" AND ");
        }
        queryStringBuilder.delete(queryStringBuilder.length()-5,queryStringBuilder.length()-1);//remove the last 'AND'
    }

    protected void generateGroupBy(SelectStmt selectStmt, StringBuilder queryStringBuilder, List<String> groupBy) {
        if(groupBy != null && !groupBy.isEmpty()){
            throw Error.get(RDBMSConstants.ERR_NO_GROUPBY, "no handler");
        }
    }

    protected void generateOrderBy(SelectStmt selectStmt, StringBuilder queryStringBuilder, List<String> orderBy) {
        if(orderBy != null && !orderBy.isEmpty()) {
            queryStringBuilder.append("ORDER BY ");
            for (String order : orderBy) {
                queryStringBuilder.append(order).append(" ,");
            }
            queryStringBuilder.deleteCharAt(queryStringBuilder.length() - 1); //remove the last ','
        }
    }

    protected void generateLimitOffset(SelectStmt selectStmt, StringBuilder queryStringBuilder, Range range) {
        Long limit = range.getLimit();
        Long offset = range.getOffset();
        if (limit != null && offset != null) {
            queryStringBuilder.append("LIMIT ").append(Long.toString(limit)).append(" OFFSET ").append(Long.toString(offset)).append(" ");
        } else if (limit != null) {
            queryStringBuilder.append("LIMIT ").append(Long.toString(limit)).append(" ");
        } else if (offset != null) {
            queryStringBuilder.append("OFFSET ").append(Long.toString(offset)).append(" ");
        }
    }

    protected void generatePos(SelectStmt selectStmt, StringBuilder queryStringBuilder) {
        // by default, intend nothing
    }

    protected class TranslationContext {
        CRUDOperationContext crudOperationContext;
        RDBMSContext rdbmsContext;
        FieldTreeNode fieldTreeNode;
        Map<String, ProjectionMapping> fieldToProjectionMap;
        Map<ProjectionMapping, Join> projectionToJoinMap;
        Map<String, ColumnToField> fieldToTablePkMap;
        SelectStmt sortDependencies;
        Set<String> nameOfTables;
        boolean needDistinct;
        boolean notOp;

        // temporary variables
        Path tmpArray;
        Type tmpType;
        List<Value> tmpValues;

        public boolean hasJoins;
        public boolean hasSortOrLimit;

        LinkedList<SelectStmt> firstStmts; // Useful for complex queries which need to run before the  main one
        SelectStmt baseStmt;
        List<Map.Entry<String,List<String>>> logicalStmt;

        public TranslationContext(RDBMSContext rdbmsContext, FieldTreeNode fieldTreeNode) {
            this.firstStmts = new LinkedList<>();
            this.fieldToProjectionMap = new HashMap<>();
            this.fieldToTablePkMap = new HashMap<>();
            this.sortDependencies = new SelectStmt(Translator.this);
            this.sortDependencies.setOrderBy(new ArrayList<String>());
            this.projectionToJoinMap = new HashMap<>();
            this.nameOfTables = new HashSet<>();
            this.baseStmt =  new SelectStmt(Translator.this);
            this.logicalStmt =  new ArrayList<>();
            this.crudOperationContext = rdbmsContext.getCrudOperationContext();
            this.rdbmsContext = rdbmsContext;
            this.fieldTreeNode = fieldTreeNode;
            index();
        }

        public List<SelectStmt> generateFinalTranslation(){
            ArrayList<SelectStmt> result = new ArrayList<>();
            SelectStmt lastStmt = new SelectStmt(Translator.this);

            for (SelectStmt stmt : firstStmts) {
                fillDefault(stmt);
                result.add(stmt);
            }

            Projection p = rdbmsContext.getProjection();
            List<String> resultColumns = new ArrayList<>();
            processProjection(p,resultColumns);
            if(resultColumns.size() == 0){
                if(rdbmsContext.getCRUDOperationName() != LightblueOperators.DELETE) {
                    throw Error.get(RDBMSConstants.ERR_NO_PROJECTION, p != null ? p.toString() : "Projection is null");
                } else{
                    rdbmsContext.getIn();
                    for (Object o : rdbmsContext.getIn()) {
                        InOut io = (InOut)o;
                        String column = fieldToProjectionMap.get(io.getField().toString()).getColumn();
                        resultColumns.add(column);
                    }
                    if(resultColumns.size() == 0){
                        throw Error.get(RDBMSConstants.ERR_ILL_FORMED_METADATA, "Delete operation need In variables to process");
                    }
                }
            }
            lastStmt.setResultColumns(resultColumns);
            fillDefault(lastStmt);
            result.add(lastStmt);

            return result;
        }

        private void fillDefault(SelectStmt selectStmt) {
            selectStmt.setFromTables(baseStmt.getFromTables());
            selectStmt.setWhereConditionals(baseStmt.getWhereConditionals());
            selectStmt.setOrderBy(sortDependencies.getOrderBy());
            selectStmt.setRange(rdbmsContext.getFromToQueryRange());
        }

        private void processProjection(Projection projection, List<String> resultColumns) {
            if(projection instanceof ProjectionList){
                ProjectionList projectionList = (ProjectionList) projection;
                for (Projection projection1 : projectionList.getItems()) {
                    processProjection(projection1,resultColumns);
                }
            }else if (projection instanceof ArrayRangeProjection) {
                ArrayRangeProjection i = (ArrayRangeProjection) projection;
                throw Error.get(RDBMSConstants.ERR_SUP_OPERATOR, projection.toString());
            }else if (projection instanceof ArrayQueryMatchProjection) {
                ArrayQueryMatchProjection i = (ArrayQueryMatchProjection) projection;
                throw Error.get(RDBMSConstants.ERR_SUP_OPERATOR, projection.toString());
            }else if (projection instanceof FieldProjection) {
                FieldProjection fieldProjection = (FieldProjection) projection;
                String sField = translatePath(fieldProjection.getField());
                String column = fieldToProjectionMap.get(sField).getColumn();

                InOut in = new InOut();
                InOut out = new InOut();
                in.setColumn(column);
                out.setColumn(column);
                in.setField(fieldProjection.getField());
                out.setField(fieldProjection.getField());

                this.rdbmsContext.getIn().add(in);
                this.rdbmsContext.getOut().add(out);
                resultColumns.add(column);
            }
        }

        private void index() {
            for (Join join : rdbmsContext.getRdbms().getSQLMapping().getJoins()) {
                for (ProjectionMapping projectionMapping : join.getProjectionMappings()) {
                    String field = projectionMapping.getField();
                    fieldToProjectionMap.put(field, projectionMapping);
                    projectionToJoinMap.put(projectionMapping, join);
                }
                needDistinct = join.isNeedDistinct() || needDistinct;
            }
            for (ColumnToField columnToField : rdbmsContext.getRdbms().getSQLMapping().getColumnToFieldMap()) {
                fieldToTablePkMap.put(columnToField.getField(), columnToField);
            }
        }

        public void clearTmp() {
            this.tmpArray = null;
            this.tmpType = null;
            this.tmpValues = null;
        }

        public void clearAll(){
            firstStmts.clear();
            fieldToProjectionMap.clear();
            this.firstStmts = null;
            this.crudOperationContext = null;
            this.rdbmsContext = null;
            this.fieldTreeNode = null;
            this.clearTmp();
        }

        public void checkJoins() {
            if (nameOfTables.size() > 1) {
                hasJoins = true;
            }
        }
    }

    public List<SelectStmt>
    translate(RDBMSContext rdbmsContext) {
        LOGGER.debug("translate {}", rdbmsContext.getQueryExpression());
        com.redhat.lightblue.util.Error.push("translateQuery");
        FieldTreeNode fieldTreeNode = rdbmsContext.getEntityMetadata().getFieldTreeRoot();

        try {
            TranslationContext translationContext = new TranslationContext(rdbmsContext, fieldTreeNode);
            preProcess(translationContext);
            recursiveTranslateQuery(translationContext, rdbmsContext.getQueryExpression());
            posProcess(translationContext);
            List<SelectStmt> translation = translationContext.generateFinalTranslation();
            return translation;

        } catch (Exception e) {
            // throw new Error (preserves current error context)
            LOGGER.error(e.getMessage(), e);
            if(e instanceof com.redhat.lightblue.util.Error){
                throw e;
            }
            throw com.redhat.lightblue.util.Error.get(RDBMSConstants.ERR_ILL_FORMED_METADATA, e.getMessage());
        } finally {
            com.redhat.lightblue.util.Error.pop();
        }
    }

    private void posProcess(TranslationContext translationContext) {
        translationContext.checkJoins();
    }

    private void preProcess(TranslationContext translationContext) {
        translateSort(translationContext);
        translateFromTo(translationContext);
    }

    protected void translateFromTo(TranslationContext translationContext) {
        if(translationContext.rdbmsContext.getFromToQueryRange().isConfigured()){
            translationContext.hasSortOrLimit = true;
        }
    }

    protected void translateSort(TranslationContext translationContext) {
        Sort sort = translationContext.rdbmsContext.getSort();
        if(sort != null) {
            if (sort instanceof CompositeSortKey) {
                CompositeSortKey c = (CompositeSortKey) sort;
                for (SortKey k : c.getKeys()) {
                    translateSortKey(translationContext, k);
                }
            } else {
                SortKey k = (SortKey) sort;
                translateSortKey(translationContext, k);
            }
        }
    }

    protected void translateSortKey(TranslationContext translationContext, SortKey sortKey) {
        String translatePath = translatePath(sortKey.getField());
        ProjectionMapping projectionMapping = translationContext.fieldToProjectionMap.get(translatePath);
        String field;
        if (projectionMapping.getSort() != null && projectionMapping.getSort().isEmpty()) {
            field = projectionMapping.getSort();
        } else {
            field = projectionMapping.getColumn();
        }
        translationContext.sortDependencies.getOrderBy().add(field);
        translationContext.hasSortOrLimit = true;
    }

    protected void recursiveTranslateQuery(TranslationContext translationContext, QueryExpression queryExpression) {
        if (queryExpression instanceof ArrayContainsExpression) {
            recursiveTranslateArrayContains(translationContext, (ArrayContainsExpression) queryExpression);
        } else if (queryExpression instanceof ArrayMatchExpression) {
            recursiveTranslateArrayElemMatch(translationContext, (ArrayMatchExpression) queryExpression);
        } else if (queryExpression instanceof FieldComparisonExpression) {
            recursiveTranslateFieldComparison(translationContext, (FieldComparisonExpression) queryExpression);
        } else if (queryExpression instanceof NaryLogicalExpression) {
            recursiveTranslateNaryLogicalExpression(translationContext, (NaryLogicalExpression) queryExpression);
        } else if (queryExpression instanceof NaryRelationalExpression) {
            recursiveTranslateNaryRelationalExpression(translationContext, (NaryRelationalExpression) queryExpression);
        } else if (queryExpression instanceof RegexMatchExpression) {
            recursiveTranslateRegexMatchExpression(translationContext, (RegexMatchExpression) queryExpression);
        } else if (queryExpression instanceof UnaryLogicalExpression) {
            recursiveTranslateUnaryLogicalExpression(translationContext, (UnaryLogicalExpression) queryExpression);
        } else if (queryExpression instanceof ValueComparisonExpression) {
            recursiveTranslateValueComparisonExpression(translationContext, (ValueComparisonExpression) queryExpression);
        } else {
            throw Error.get(RDBMSConstants.ERR_SUP_QUERY, queryExpression!=null?queryExpression.toString():"q=null");
        }
    }

    protected FieldTreeNode resolve(FieldTreeNode fieldTreeNode, Path path) {
        FieldTreeNode fieldTreeNode1 = fieldTreeNode.resolve(path);
        if (fieldTreeNode1 == null) {
            throw com.redhat.lightblue.util.Error.get(RDBMSConstants.INV_FIELD, path.toString());
        }
        return fieldTreeNode1;
    }

    protected static String translatePath(Path p) {
        StringBuilder stringBuilder = new StringBuilder();
        int n = p.numSegments();
        for (int i = 0; i < n; i++) {
            String head = p.head(i);
            if (!head.equals(Path.ANY)) {
                if (i > 0) {
                    stringBuilder.append('.');
                }
                stringBuilder.append(head);
            }
        }
        return stringBuilder.toString();
    }

    protected List<Object> translateValueList(Type type, List<Value> values) {
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("Empty values");
        }
        List<Object> objectList = new ArrayList<>(values.size());
        for (Value v : values) {
            Object value = v == null ? null : v.getValue();
            if (value != null) {
                value = type.cast(value);
            }
            objectList.add(value);
        }
        return objectList;
    }

    protected void recursiveTranslateArrayContains(TranslationContext translationContext, ArrayContainsExpression arrayContainsExpression) {
        FieldTreeNode arrayNode = resolve(translationContext.fieldTreeNode, arrayContainsExpression.getArray());
        if (arrayNode instanceof ArrayField) {
            translationContext.tmpType = ((ArrayField) arrayNode).getElement().getType();
            translationContext.tmpArray = arrayContainsExpression.getArray();
            translationContext.tmpValues = arrayContainsExpression.getValues();
            String operation;
            switch (arrayContainsExpression.getOp()) {
                case _all:
                    operation = !translationContext.notOp?"IN":"NOT IN";
                    break;
                case _any:
                    operation = null; //OR
                    break;
                case _none:
                    operation = !translationContext.notOp?"NOT IN":"IN";
                    break;
                default:
                    throw com.redhat.lightblue.util.Error.get(RDBMSConstants.NO_FIELD, arrayContainsExpression.toString());
            }
            Type t = ((ArrayField)resolve(translationContext.fieldTreeNode, arrayContainsExpression.getArray())).getElement().getType();
            if(operation != null) {
                List<Object> values = translateValueList(t, arrayContainsExpression.getValues());
                String field = arrayContainsExpression.getArray().toString();
                ProjectionMapping projectionMapping = translationContext.fieldToProjectionMap.get(field);
                Join join = translationContext.projectionToJoinMap.get(projectionMapping);
                fillTables(translationContext, translationContext.baseStmt.getFromTables(), join);
                fillWhere(translationContext, translationContext.baseStmt.getWhereConditionals(), join);
                String result = null;
                if (t.supportsEq()) {
                    result = projectionMapping.getColumn() + " " + operation + " " + "('" + StringUtils.join(values, "','") + "')";
                }else{
                    for (int i = 0; i < values.size(); i++) {
                        Object v = values.get(i);
                        result = result + projectionMapping.getColumn() + " = " +v;
                        if(i != values.size()-1){
                            result = result +" OR ";
                        }
                    }
                }
                addConditional(translationContext, result);
            } else {
                throw Error.get(RDBMSConstants.ERR_NO_OPERATOR, arrayContainsExpression.toString());
            }
            translationContext.clearTmp();
        } else {
            throw com.redhat.lightblue.util.Error.get(RDBMSConstants.INV_FIELD, arrayContainsExpression.toString());
        }
    }

    //Possible subquery or it will need to run a query before this
    //{ _id: 1, results: [ 82, 85, 88 ] } { _id: 2, results: [ 75, 88, 89 ] } ->{ results: { elemMatch: { gte: 80, lt: 85 } } }->{ "_id" : 1, "results" : [ 82, 85, 88 ] }
    protected void recursiveTranslateArrayElemMatch(TranslationContext translationContext, ArrayMatchExpression arrayMatchExpression) {
        FieldTreeNode arrayNode = resolve(translationContext.fieldTreeNode, arrayMatchExpression.getArray());
        if (arrayNode instanceof ArrayField) {
            ArrayElement arrayElement = ((ArrayField) arrayNode).getElement();
            if (arrayElement instanceof ObjectArrayElement) {
                FieldTreeNode tmpFieldTreeNode = translationContext.fieldTreeNode;
                translationContext.fieldTreeNode = arrayElement;
                recursiveTranslateQuery(translationContext, arrayMatchExpression.getElemMatch());
                String path = translatePath(arrayMatchExpression.getArray());
                // TODO Need to define what would happen in this scenario (not supported yet)
                translationContext.fieldTreeNode = tmpFieldTreeNode;
                throw Error.get(RDBMSConstants.ERR_NO_OPERATOR, arrayMatchExpression.toString());
            }
        }
        throw com.redhat.lightblue.util.Error.get(RDBMSConstants.INV_FIELD, arrayMatchExpression.toString());
    }

    protected void recursiveTranslateFieldComparison(TranslationContext translationContext, FieldComparisonExpression fieldComparisonExpression) {
        // We have to deal with array references here
        Path rField = fieldComparisonExpression.getRfield();
        Path lField = fieldComparisonExpression.getField();
        int rAnys = rField.nAnys();
        int lAnys = lField.nAnys();
        if (rAnys > 0 && lAnys > 0) {
            // TODO Need to define what would happen in this scenario
            throw Error.get(RDBMSConstants.ERR_NO_OPERATOR, fieldComparisonExpression.toString());
        } else if (rAnys > 0 || lAnys > 0) {
            // TODO Need to define what would happen in this scenario
            throw Error.get(RDBMSConstants.ERR_NO_OPERATOR, fieldComparisonExpression.toString());
        } else {
            // No ANYs, direct comparison
            String field = fieldComparisonExpression.getField().toString();
            String rfield = fieldComparisonExpression.getRfield().toString();

            ProjectionMapping projectionMapping = translationContext.fieldToProjectionMap.get(field);
            Join join = translationContext.projectionToJoinMap.get(projectionMapping);
            fillTables(translationContext, translationContext.baseStmt.getFromTables(), join);
            fillWhere(translationContext, translationContext.baseStmt.getWhereConditionals(), join);

            ProjectionMapping projectionMapping1 = translationContext.fieldToProjectionMap.get(rfield);
            Join join1 = translationContext.projectionToJoinMap.get(projectionMapping1);
            fillTables(translationContext, translationContext.baseStmt.getFromTables(), join1);
            fillWhere(translationContext, translationContext.baseStmt.getWhereConditionals(), join1);

            String operator = !translationContext.notOp? BINARY_TO_SQL.get(fieldComparisonExpression.getOp()): NOTBINARY_TO_SQL.get(fieldComparisonExpression.getOp());
            String result = projectionMapping.getColumn() + " " + operator + " " + projectionMapping1.getColumn();
            addConditional(translationContext, result);
        }
    }

    protected void addConditional(TranslationContext translationContext, String conditional) {
        if(translationContext.logicalStmt.size() > 0){
            translationContext.logicalStmt.get(translationContext.logicalStmt.size()-1).getValue().add(conditional);
        } else {
            translationContext.baseStmt.getWhereConditionals().add(conditional);
        }
    }

    protected void fillWhere(TranslationContext translationContext, List<String> wheres, Join join) {
        if(join.getJoinTablesStatement() != null && !join.getJoinTablesStatement().isEmpty()) {
            wheres.add(join.getJoinTablesStatement());
        }
    }

    protected void fillTables(TranslationContext translationContext, List<String> fromTables, Join join) {
        for (Table table : join.getTables()) {
            if(translationContext.nameOfTables.add(table.getName())){
                LOGGER.warn("Table mentioned more than once in the same query. Possible N+1 problem");
            }
            if(table.getAlias() != null && !table.getAlias().isEmpty() ){
                fromTables.add(table.getName() + " AS " + table.getAlias() );
            } else {
                fromTables.add(table.getName());
            }
        }
    }

    protected void recursiveTranslateNaryLogicalExpression(TranslationContext translationContext, NaryLogicalExpression naryLogicalExpression){
        String ops = NARY_TO_SQL.get(naryLogicalExpression.getOp());
        boolean noStatement = translationContext.logicalStmt.size() == 0;
        translationContext.logicalStmt.add( new AbstractMap.SimpleEntry<String,List<String>>(ops, new ArrayList<String>()));
        for (QueryExpression queryExpression : naryLogicalExpression.getQueries()) {
            recursiveTranslateQuery(translationContext,queryExpression);
        }
        Map.Entry<String, List<String>> remove = translationContext.logicalStmt.remove(translationContext.logicalStmt.size()-1);
        String op = remove.getKey() + " ";
        StringBuilder conditionalStringBuilder = new StringBuilder();
        if(!noStatement || !translationContext.baseStmt.getWhereConditionals().isEmpty()){
            conditionalStringBuilder.append("(");
        }
        for (int i = 0; i < remove.getValue().size() ; i++) {
            String value = remove.getValue().get(i);
            if(i == (remove.getValue().size()-1)) {
                conditionalStringBuilder.append(value);
                if(!noStatement || !translationContext.baseStmt.getWhereConditionals().isEmpty()){
                    conditionalStringBuilder.append(") ");
                }
            } else {
                conditionalStringBuilder.append(value).append(" ").append(op);
            }
        }
        if(noStatement) {
            translationContext.baseStmt.getWhereConditionals().add(conditionalStringBuilder.toString());
        } else {
            translationContext.logicalStmt.get(translationContext.logicalStmt.size() - 1).getValue().add(conditionalStringBuilder.toString());
        }
    }

    protected void recursiveTranslateNaryRelationalExpression(TranslationContext translationContext, NaryRelationalExpression naryRelationalExpression){
        Type t = resolve(translationContext.fieldTreeNode, naryRelationalExpression.getField()).getType();
        if (t.supportsEq()) {
            List<Object> values = translateValueList(t, naryRelationalExpression.getValues());
            String field = naryRelationalExpression.getField().toString();
            String operator = naryRelationalExpression.getOp().toString().equals(NaryRelationalOperator._in.toString()) ?"IN" : "NOT IN";

            ProjectionMapping fpm = translationContext.fieldToProjectionMap.get(field);
            Join fJoin = translationContext.projectionToJoinMap.get(fpm);
            fillTables(translationContext, translationContext.baseStmt.getFromTables(), fJoin);
            fillWhere(translationContext, translationContext.baseStmt.getWhereConditionals(), fJoin);
            String result = fpm.getColumn() + " " + operator + " " +  "('" +StringUtils.join(values, "','")+"')";
            addConditional(translationContext, result);
        } else {
            throw Error.get(RDBMSConstants.INV_FIELD, naryRelationalExpression.toString());
        }
    }

    protected void recursiveTranslateRegexMatchExpression(TranslationContext translationContext,RegexMatchExpression regexMatchExpression){
        throw Error.get(RDBMSConstants.ERR_NO_OPERATOR, regexMatchExpression.toString());
    }

    protected void recursiveTranslateUnaryLogicalExpression(TranslationContext translationContext, UnaryLogicalExpression unaryLogicalExpression){
        translationContext.notOp = !translationContext.notOp;
        recursiveTranslateQuery(translationContext, unaryLogicalExpression.getQuery());
        translationContext.notOp = !translationContext.notOp;
    }

    protected void recursiveTranslateValueComparisonExpression(TranslationContext translationContext, ValueComparisonExpression valueComparisonExpression){
        StringBuilder str = new StringBuilder();
        // We have to deal with array references here
        Value rvalue = valueComparisonExpression.getRvalue();
        Path lField = valueComparisonExpression.getField();
        int ln = lField.nAnys();
        if (ln > 0) {
            // TODO Need to define what would happen in this scenario
            throw Error.get(RDBMSConstants.ERR_NO_OPERATOR, valueComparisonExpression.toString());
        } else if (ln > 0) {
            // TODO Need to define what would happen in this scenario
            throw Error.get(RDBMSConstants.ERR_NO_OPERATOR, valueComparisonExpression.toString());
        } else {
            // No ANYs, direct comparison
            String field = lField.toString();
            String value = rvalue.toString();

            ProjectionMapping projectionMapping = translationContext.fieldToProjectionMap.get(field);
            Join join = translationContext.projectionToJoinMap.get(projectionMapping);
            fillTables(translationContext, translationContext.baseStmt.getFromTables(), join);
            fillWhere(translationContext, translationContext.baseStmt.getWhereConditionals(), join);

            String operator = !translationContext.notOp? BINARY_TO_SQL.get(valueComparisonExpression.getOp()): NOTBINARY_TO_SQL.get(valueComparisonExpression.getOp());
            value = value.replaceAll("\"","'");
            String conditional = projectionMapping.getColumn() + " " + operator + " " + value;
            addConditional(translationContext, conditional);
        }
    }
}
