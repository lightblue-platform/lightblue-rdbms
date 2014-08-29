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

import com.redhat.lightblue.crud.CRUDOperationContext;
import com.redhat.lightblue.metadata.*;
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

    public String generateStatement(SelectStmt s) {
        StringBuilder queryStr = new StringBuilder();
        generatePre(s, queryStr);
        generateResultColumns(s, queryStr, s.getResultColumns());
        generateFrom(s, queryStr, s.getFromTables());
        generateWhere(s, queryStr, s.getWhereConditionals());
        generateGroupBy(s, queryStr, s.getGroupBy());
        generateOrderBy(s, queryStr, s.getOrderBy());
        generateLimitOffset(s, queryStr, s.getLimit(), s.getOffset());
        generatePos(s, queryStr);

        return queryStr.toString();
    }

    protected void generatePre(SelectStmt s, StringBuilder queryStr) {
        queryStr.append("SELECT ");
        if(s.getDistic()){
            queryStr.append("DISTINCT ");
        }
    }

    protected void generateResultColumns(SelectStmt s, StringBuilder queryStr, List<String> resultColumns) {
        for (String resultColumn : resultColumns) {
            queryStr.append(resultColumn).append(" ,");
        }
        queryStr.deleteCharAt(queryStr.length() - 1); //remove the last ','
    }

    protected void generateFrom(SelectStmt s, StringBuilder queryStr, List<String> fromTables) {
        queryStr.append("FROM ");
        for (String table : fromTables) {
            queryStr.append(table).append(" ,");
        }
        queryStr.deleteCharAt(queryStr.length()-1); //remove the last ','
    }

    protected void generateWhere(SelectStmt s, StringBuilder queryStr, LinkedList<String> whereConditionals) {
        queryStr.append("WHERE ");
        for (String where : whereConditionals) {
            queryStr.append(where).append(" AND ");
        }
        queryStr.deleteCharAt(queryStr.length()-1); //remove the last 'AND'
        queryStr.deleteCharAt(queryStr.length()-1); //remove the last 'AND'
        queryStr.deleteCharAt(queryStr.length()-1); //remove the last 'AND'
        queryStr.deleteCharAt(queryStr.length()-1); //remove the last 'AND'
    }

    protected void generateGroupBy(SelectStmt s, StringBuilder queryStr, List<String> groupBy) {
        if(groupBy != null && groupBy.size() < 0){
            throw Error.get("GroupBy not supported", "no handler");
        }
    }

    protected void generateOrderBy(SelectStmt s, StringBuilder queryStr, List<String> orderBy) {
        if(orderBy != null && orderBy.size() < 0) {
            queryStr.append("ORDER BY ");
            for (String order : orderBy) {
                queryStr.append(order).append(" ,");
            }
            queryStr.deleteCharAt(queryStr.length() - 1); //remove the last ',
        }
    }

    protected void generateLimitOffset(SelectStmt s, StringBuilder queryStr, Long limit, Long offset) {
        if (limit != null && offset != null) {
            queryStr.append("LIMIT ").append(Long.toString(limit)).append(" OFFSET ").append(Long.toString(offset)).append(" ");
        } else if (limit != null) {
            queryStr.append("LIMIT ").append(Long.toString(limit)).append(" ");
        } else if (offset != null) {
            queryStr.append("OFFSET ").append(Long.toString(offset)).append(" ");
        }
    }

    protected void generatePos(SelectStmt s, StringBuilder queryStr) {
        // by default, intend nothing
    }

    protected class TranslationContext {
        CRUDOperationContext c;
        RDBMSContext r;
        FieldTreeNode f;
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

        public TranslationContext(RDBMSContext r, FieldTreeNode f) {
            this.firstStmts = new LinkedList<>();
            this.fieldToProjectionMap = new HashMap<>();
            this.fieldToTablePkMap = new HashMap<>();
            this.sortDependencies = new SelectStmt(Translator.this);
            this.sortDependencies.setOrderBy(new ArrayList<String>());
            this.projectionToJoinMap = new HashMap<>();
            this.nameOfTables = new HashSet<>();
            this.baseStmt =  new SelectStmt(Translator.this);
            this.logicalStmt =  new ArrayList<>();
            this.c = r.getCrudOperationContext();
            this.r = r;
            this.f = f;
            index();
        }

        public List<SelectStmt> generateFinalTranslation(){
            ArrayList<SelectStmt> result = new ArrayList<>();
            SelectStmt lastStmt = new SelectStmt(Translator.this);

            for (SelectStmt stmt : firstStmts) {
                fillDefault(stmt);
                result.add(stmt);
            }

            Projection p = r.getProjection();
            List<String> l = new ArrayList<>();
            processProjection(p,l);
            if(l.size() == 0){
                throw Error.get("no projection", p.toString());
            }
            lastStmt.setResultColumns(l);
            fillDefault(lastStmt);
            result.add(lastStmt);

            return result;
        }

        private void fillDefault(SelectStmt stmt) {
            stmt.setFromTables(baseStmt.getFromTables());
            stmt.setWhereConditionals(baseStmt.getWhereConditionals());
            stmt.setOrderBy(sortDependencies.getOrderBy());
            stmt.setOffset(r.getFrom());
            if(r.getTo() != null) {
                stmt.setLimit(r.getTo() - r.getFrom()); // after the offset (M rows skipped), the remaining will be limited
            }else{
                stmt.setLimit(r.getTo());
            }
        }

        private void processProjection(Projection p, List<String> l) {
            if(p instanceof ProjectionList){
                ProjectionList i = (ProjectionList) p;
                for (Projection pi : i.getItems()) {
                    processProjection(pi,l);
                }
            }else if (p instanceof ArrayRangeProjection) {
                ArrayRangeProjection i = (ArrayRangeProjection) p;
                throw Error.get("not supported projection", p.toString());
            }else if (p instanceof ArrayQueryMatchProjection) {
                ArrayQueryMatchProjection i = (ArrayQueryMatchProjection) p;
                throw Error.get("not supported projection", p.toString());
            }else if (p instanceof FieldProjection) {
                FieldProjection i = (FieldProjection) p;
                String sField = translatePath(i.getField());
                String column = fieldToProjectionMap.get(sField).getColumn();

                InOut in = new InOut();
                InOut out = new InOut();
                in.setColumn(column);
                out.setColumn(column);
                in.setField(i.getField());
                out.setField(i.getField());

                this.r.getIn().add(in);
                this.r.getOut().add(out);
                l.add(column);
            }
        }

        private void index() {
            for (Join join : r.getRdbms().getSQLMapping().getJoins()) {
                for (ProjectionMapping projectionMapping : join.getProjectionMappings()) {
                    String field = projectionMapping.getField();
                    fieldToProjectionMap.put(field, projectionMapping);
                    projectionToJoinMap.put(projectionMapping, join);
                }
                needDistinct = join.isNeedDistinct() || needDistinct;
            }
            for (ColumnToField columnToField : r.getRdbms().getSQLMapping().getColumnToFieldMap()) {
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
            this.c = null;
            this.r = null;
            this.f = null;
            this.clearTmp();
        }

        public void checkJoins() {
            if (nameOfTables.size() > 1) {
                hasJoins = true;
            }
        }
    }

    public List<SelectStmt> translate(RDBMSContext r) {
        LOGGER.debug("translate {}", r.getQueryExpression());
        com.redhat.lightblue.util.Error.push("translateQuery");
        FieldTreeNode f = r.getEntityMetadata().getFieldTreeRoot();

        try {
            TranslationContext translationContext = new TranslationContext(r, f);
            preProcess(translationContext);
            recursiveTranslateQuery(translationContext,r.getQueryExpression());
            posProcess(translationContext);
            List<SelectStmt> translation = translationContext.generateFinalTranslation();
            return translation;

        } catch (Exception e) {
            // throw new Error (preserves current error context)
            LOGGER.error(e.getMessage(), e);
            if(e instanceof com.redhat.lightblue.util.Error){
                throw e;
            }
            throw com.redhat.lightblue.util.Error.get("Invalid Object!", e.getMessage());
        } finally {
            com.redhat.lightblue.util.Error.pop();
        }
    }

    private void posProcess(TranslationContext t) {
        t.checkJoins();
    }

    private void preProcess(TranslationContext t) {
        translateSort(t);
        translateFromTo(t);
    }

    protected void translateFromTo(TranslationContext t) {
        if(t.r.getTo() != null || t.r.getFrom() != null){
            t.hasSortOrLimit = true;
        }
    }

    protected void translateSort(TranslationContext translationContext) {
        Sort sort = translationContext.r.getSort();
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

    protected void translateSortKey(TranslationContext t, SortKey k) {
        String translatePath = translatePath(k.getField());
        ProjectionMapping projectionMapping = t.fieldToProjectionMap.get(translatePath);
        String field;
        if (projectionMapping.getSort() != null && projectionMapping.getSort().isEmpty()) {
            field = projectionMapping.getSort();
        } else {
            field = projectionMapping.getColumn();
        }
        t.sortDependencies.getOrderBy().add(field);
        t.hasSortOrLimit = true;
    }

    protected void recursiveTranslateQuery(TranslationContext c, QueryExpression q) {
        if (q instanceof ArrayContainsExpression) {
            recursiveTranslateArrayContains(c, (ArrayContainsExpression) q);
        } else if (q instanceof ArrayMatchExpression) {
            recursiveTranslateArrayElemMatch(c, (ArrayMatchExpression) q);
        } else if (q instanceof FieldComparisonExpression) {
            recursiveTranslateFieldComparison(c, (FieldComparisonExpression) q);
        } else if (q instanceof NaryLogicalExpression) {
            recursiveTranslateNaryLogicalExpression(c, (NaryLogicalExpression) q);
        } else if (q instanceof NaryRelationalExpression) {
            recursiveTranslateNaryRelationalExpression(c, (NaryRelationalExpression) q);
        } else if (q instanceof RegexMatchExpression) {
            recursiveTranslateRegexMatchExpression(c, (RegexMatchExpression) q);
        } else if (q instanceof UnaryLogicalExpression) {
            recursiveTranslateUnaryLogicalExpression(c, (UnaryLogicalExpression) q);
        } else if (q instanceof ValueComparisonExpression) {
            recursiveTranslateValueComparisonExpression(c, (ValueComparisonExpression) q);
        } else {
            throw Error.get("Not supported query", q!=null?q.toString():"q=null");
        }
    }

    protected FieldTreeNode resolve(FieldTreeNode fieldTreeNode, Path path) {
        FieldTreeNode node = fieldTreeNode.resolve(path);
        if (node == null) {
            throw com.redhat.lightblue.util.Error.get("Invalid field", path.toString());
        }
        return node;
    }

    protected static String translatePath(Path p) {
        StringBuilder str = new StringBuilder();
        int n = p.numSegments();
        for (int i = 0; i < n; i++) {
            String s = p.head(i);
            if (!s.equals(Path.ANY)) {
                if (i > 0) {
                    str.append('.');
                }
                str.append(s);
            }
        }
        return str.toString();
    }

    protected List<Object> translateValueList(Type t, List<Value> values) {
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("Empty values");
        }
        List<Object> ret = new ArrayList<>(values.size());
        for (Value v : values) {
            Object value = v == null ? null : v.getValue();
            if (value != null) {
                value = t.cast(value);
            }
            ret.add(value);
        }
        return ret;
    }

    protected void recursiveTranslateArrayContains(TranslationContext c, ArrayContainsExpression expr) {
        FieldTreeNode arrayNode = resolve(c.f, expr.getArray());
        if (arrayNode instanceof ArrayField) {
            c.tmpType = ((ArrayField) arrayNode).getElement().getType();
            c.tmpArray = expr.getArray();
            c.tmpValues = expr.getValues();
            String op;
            switch (expr.getOp()) {
                case _all:
                    op = !c.notOp?"IN":"NOT IN";
                    break;
                case _any:
                    op = null; //OR
                    break;
                case _none:
                    op = !c.notOp?"NOT IN":"IN";
                    break;
                default:
                    throw com.redhat.lightblue.util.Error.get("Not mapped field", expr.toString());
            }
            Type t = ((ArrayField)resolve(c.f, expr.getArray())).getElement().getType();
            if(op != null) {
                List<Object> values = translateValueList(t, expr.getValues());
                String f = expr.getArray().toString();
                ProjectionMapping fpm = c.fieldToProjectionMap.get(f);
                Join fJoin = c.projectionToJoinMap.get(fpm);
                fillTables(c, c.baseStmt.getFromTables(), fJoin);
                fillWhere(c, c.baseStmt.getWhereConditionals(), fJoin);
                String s = null;
                if (t.supportsEq()) {
                    s = fpm.getColumn() + " " + op + " " + "(\"" + StringUtils.join(values, "\",\"") + "\")";
                }else{
                    for (int i = 0; i < values.size(); i++) {
                        Object v = values.get(i);
                        s = s + fpm.getColumn() + " = " +v;
                        if(i != values.size()-1){
                            s = s +" OR ";
                        }
                    }
                }
                addConditional(c, s);
            } else {
                throw Error.get("not supported operator", expr.toString());
            }
            c.clearTmp();
        } else {
            throw com.redhat.lightblue.util.Error.get("Invalid field", expr.toString());
        }
    }

    //Possible subquery or it will need to run a query before this
    //{ _id: 1, results: [ 82, 85, 88 ] } { _id: 2, results: [ 75, 88, 89 ] } ->{ results: { $elemMatch: { $gte: 80, $lt: 85 } } }->{ "_id" : 1, "results" : [ 82, 85, 88 ] }
    protected void recursiveTranslateArrayElemMatch(TranslationContext c, ArrayMatchExpression expr) {
        FieldTreeNode arrayNode = resolve(c.f, expr.getArray());
        if (arrayNode instanceof ArrayField) {
            ArrayElement el = ((ArrayField) arrayNode).getElement();
            if (el instanceof ObjectArrayElement) {
                FieldTreeNode tmp = c.f;
                c.f = el;
                recursiveTranslateQuery(c, expr.getElemMatch());
                String path = translatePath(expr.getArray());
                // TODO Need to define what would happen in this scenario (not supported yet)
                c.f = tmp;
                throw Error.get("not supported operator", expr.toString());
            }
        }
        throw com.redhat.lightblue.util.Error.get("Invalid field", expr.toString());
    }

    protected void recursiveTranslateFieldComparison(TranslationContext c, FieldComparisonExpression expr) {
        StringBuilder str = new StringBuilder();
        // We have to deal with array references here
        Path rField = expr.getRfield();
        Path lField = expr.getField();
        int rn = rField.nAnys();
        int ln = lField.nAnys();
        if (rn > 0 && ln > 0) {
            // TODO Need to define what would happen in this scenario
            throw Error.get("not supported operator", expr.toString());
        } else if (rn > 0 || ln > 0) {
            // TODO Need to define what would happen in this scenario
            throw Error.get("not supported operator", expr.toString());
        } else {
            // No ANYs, direct comparison
            String f = expr.getField().toString();
            String r = expr.getRfield().toString();

            ProjectionMapping fpm = c.fieldToProjectionMap.get(f);
            Join fJoin = c.projectionToJoinMap.get(fpm);
            fillTables(c, c.baseStmt.getFromTables(), fJoin);
            fillWhere(c, c.baseStmt.getWhereConditionals(), fJoin);

            ProjectionMapping rpm = c.fieldToProjectionMap.get(r);
            Join rJoin = c.projectionToJoinMap.get(rpm);
            fillTables(c, c.baseStmt.getFromTables(), rJoin);
            fillWhere(c, c.baseStmt.getWhereConditionals(), rJoin);

            String s1 = !c.notOp? BINARY_TO_SQL.get(expr.getOp()): NOTBINARY_TO_SQL.get(expr.getOp());
            String s = fpm.getColumn() + " " + s1 + " " + rpm.getColumn();
            addConditional(c, s);
        }
    }

    protected void addConditional(TranslationContext c, String s) {
        if(c.logicalStmt.size() > 0){
            c.logicalStmt.get(c.logicalStmt.size()-1).getValue().add(s);
        } else {
            c.baseStmt.getWhereConditionals().add(s);
        }
    }

    protected void fillWhere(TranslationContext c, List<String> wheres, Join fJoin) {
        if(fJoin.getJoinTablesStatement() != null && !fJoin.getJoinTablesStatement().isEmpty()) {
            wheres.add(fJoin.getJoinTablesStatement());
        }
    }

    protected void fillTables(TranslationContext c, List<String> fromTables, Join fJoin) {
        for (Table table : fJoin.getTables()) {
            if(c.nameOfTables.add(table.getName())){
                LOGGER.warn("Table mentioned more than once in the same query. Possible N+1 problem");
            }
            if(table.getAlias() != null && !table.getAlias().isEmpty() ){
                fromTables.add(table.getName() + " AS " + table.getAlias() );
            } else {
                fromTables.add(table.getName());
            }
        }
    }

    protected void recursiveTranslateNaryLogicalExpression(TranslationContext c, NaryLogicalExpression naryLogicalExpression){
        String ops = NARY_TO_SQL.get(naryLogicalExpression.getOp());
        boolean b = c.logicalStmt.size() == 0;
        c.logicalStmt.add( new AbstractMap.SimpleEntry<String,List<String>>(ops, new ArrayList<String>()));
        for (QueryExpression queryExpression : naryLogicalExpression.getQueries()) {
            recursiveTranslateQuery(c,queryExpression);
        }
        Map.Entry<String, List<String>> remove = c.logicalStmt.remove(c.logicalStmt.size()-1);
        String op = remove.getKey() + " ";
        StringBuilder sb = new StringBuilder();
        if(!b || c.baseStmt.getWhereConditionals().size() > 0){
            sb.append("(");
        }
        for (int i = 0; i < remove.getValue().size() ; i++) {
            String s = remove.getValue().get(i);
            if(i == (remove.getValue().size()-1)) {
                sb.append(s);
                if(!b || c.baseStmt.getWhereConditionals().size() > 0){
                    sb.append(") ");
                }
            } else {
                sb.append(s).append(" ").append(op);
            }
        }
        if(b) {
            c.baseStmt.getWhereConditionals().add(sb.toString());
        } else {
            c.logicalStmt.get(c.logicalStmt.size()-1).getValue().add(sb.toString());
        }
    }

    protected void recursiveTranslateNaryRelationalExpression(TranslationContext c, NaryRelationalExpression expr){
        Type t = resolve(c.f, expr.getField()).getType();
        if (t.supportsEq()) {
            List<Object> values = translateValueList(t, expr.getValues());
            String f = expr.getField().toString();
            String op = expr.getOp().toString().equals("$in") ? "IN" : "NOT IN";

            ProjectionMapping fpm = c.fieldToProjectionMap.get(f);
            Join fJoin = c.projectionToJoinMap.get(fpm);
            fillTables(c, c.baseStmt.getFromTables(), fJoin);
            fillWhere(c, c.baseStmt.getWhereConditionals(), fJoin);
            String s = fpm.getColumn() + " " + op + " " +  "(\"" +StringUtils.join(values, "\",\"")+"\")";
            addConditional(c, s);
        } else {
            throw Error.get("invalid field", expr.toString());
        }
    }

    protected void recursiveTranslateRegexMatchExpression(TranslationContext c,RegexMatchExpression expr){
        throw Error.get("not supported operator", expr.toString());
    }

    protected void recursiveTranslateUnaryLogicalExpression(TranslationContext c, UnaryLogicalExpression expr){
        c.notOp = !c.notOp;
        recursiveTranslateQuery(c, expr.getQuery());
        c.notOp = !c.notOp;
    }

    protected void recursiveTranslateValueComparisonExpression(TranslationContext c, ValueComparisonExpression expr){
        StringBuilder str = new StringBuilder();
        // We have to deal with array references here
        Value rvalue = expr.getRvalue();
        Path lField = expr.getField();
        int ln = lField.nAnys();
        if (ln > 0) {
            // TODO Need to define what would happen in this scenario
            throw Error.get("not supported operator", expr.toString());
        } else if (ln > 0) {
            // TODO Need to define what would happen in this scenario
            throw Error.get("not supported operator", expr.toString());
        } else {
            // No ANYs, direct comparison
            String f = lField.toString();
            String r = rvalue.toString();

            ProjectionMapping fpm = c.fieldToProjectionMap.get(f);
            Join fJoin = c.projectionToJoinMap.get(fpm);
            fillTables(c, c.baseStmt.getFromTables(), fJoin);
            fillWhere(c, c.baseStmt.getWhereConditionals(), fJoin);

            String s1 = !c.notOp? BINARY_TO_SQL.get(expr.getOp()): NOTBINARY_TO_SQL.get(expr.getOp());
            String s = fpm.getColumn() + " " + s1 + " " + r;
            addConditional(c, s);
        }
    }
}
