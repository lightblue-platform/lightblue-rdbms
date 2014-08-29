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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.redhat.lightblue.common.rdbms.RDBMSDataStore;
import com.redhat.lightblue.metadata.rdbms.converter.DynVar;
import com.redhat.lightblue.metadata.rdbms.converter.SelectStmt;
import com.redhat.lightblue.crud.CRUDOperationContext;
import com.redhat.lightblue.hystrix.rdbms.ExecuteUpdateCommand;
import com.redhat.lightblue.metadata.rdbms.converter.RDBMSContext;
import com.redhat.lightblue.metadata.rdbms.converter.Translator;
import com.redhat.lightblue.metadata.rdbms.enums.ExpressionOperators;
import com.redhat.lightblue.metadata.rdbms.enums.IfOperators;
import com.redhat.lightblue.metadata.rdbms.model.*;
import com.redhat.lightblue.metadata.rdbms.util.Column;
import com.redhat.lightblue.util.JsonDoc;
import com.redhat.lightblue.util.Path;

import javax.sql.DataSource;
import java.util.*;

/**
 *
 * @author lcestari
 */
public class RDBMSProcessor {
    public static void process(RDBMSContext rdbmsContext) {
        RDBMS rdbms = (RDBMS) rdbmsContext.getEntityMetadata().getEntitySchema().getProperties().get("rdbms");
        if (rdbms == null) {
            throw new IllegalStateException("Configured to use RDBMS but no RDBMS definition was found for the entity");
        }
        rdbmsContext.setRdbms(rdbms);
        RDBMSDataStore d = (RDBMSDataStore) rdbmsContext.getEntityMetadata().getDataStore();
        DataSource ds = rdbmsContext.getRDBMSDataSourceResolver().get(d);
        rdbmsContext.setDataSource(ds);
        rdbmsContext.setRowMapper(new VariableUpdateRowMapper(rdbmsContext));


        //create the first SQL statements to run the RDBMS module
        List<SelectStmt> inputStmt = Translator.ORACLE.translate(rdbmsContext);

        Operation op = rdbmsContext.getRdbms().getOperationByName(rdbmsContext.getCRUDOperationName());
        op.getBindings().setInList(rdbmsContext.getIn());
        op.getBindings().setOutList(rdbmsContext.getOut());

        rdbmsContext.setInitialInput(true);
        new ExecuteUpdateCommand(rdbmsContext, inputStmt).execute();
        rdbmsContext.setInitialInput(false);

        mapInputWithBinding(rdbmsContext);

        recursiveExpressionCall(rdbmsContext, op, op.getExpressionList());

        // processed final output
        if(op.getExpressionList().isEmpty()){
            convertInputToProjection(rdbmsContext);
        } else{
            convertOutputToProjection(rdbmsContext);
        }
    }

    private static void mapInputWithBinding(RDBMSContext rdbmsContext) {
        List<InOut> in = rdbmsContext.getIn();
        DynVar inVar = rdbmsContext.getInVar();
        rdbmsContext.setInputMappedByField(new HashMap<String,List>());
        rdbmsContext.setInputMappedByColumn(new HashMap<String, List>());
        for (InOut i : in) {
            rdbmsContext.getInputMappedByColumn().put(i.getColumn(),inVar.getValues(i.getColumn()));
            rdbmsContext.getInputMappedByField().put(i.getField(), inVar.getValues(i.getField().toString()));
        }
    }

    private static void convertOutputToProjection(RDBMSContext rdbmsContext) {
        convertProjection(rdbmsContext,rdbmsContext.getOut(),rdbmsContext.getOutVar());
    }

    private static void convertInputToProjection(RDBMSContext rdbmsContext) {
        convertProjection(rdbmsContext,rdbmsContext.getIn(),rdbmsContext.getInVar());
    }
    private static void convertProjection(RDBMSContext rdbmsContext, List<InOut> inout, DynVar dynVar) {
        List<JsonDoc> l = new ArrayList<>();

        for (InOut io : inout) {
            String field = io.getField().toString();
            List values = dynVar.getValues(field);
            JsonDoc jd = null;
            if(values.isEmpty()){
                jd = new JsonDoc(NullNode.getInstance());
            }else if(values.size() > 1 ){
                jd = new JsonDoc(new TextNode(values.get(1).toString()));
            }else {
                ArrayNode doc = new ArrayNode(rdbmsContext.getJsonNodeFactory());
                for (Object value : values) {
                    doc.add(value.toString());
                }
                jd = new JsonDoc(doc);
            }
            l.add(jd);
        }

        rdbmsContext.getCrudOperationContext().addDocuments(l);
    }

    private static void recursiveExpressionCall(RDBMSContext rdbmsContext, Operation op, List<Expression> expressionList) {
        if (expressionList == null) {
            return;
        }
        for (Expression expression : expressionList) {
            final String simpleName = expression.getClass().getSimpleName();
            switch (simpleName) {
                case ExpressionOperators.CONDITIONAL:
                    Conditional c = (Conditional) expression;
                    recursiveConditionalCall(rdbmsContext, op, expressionList, c);
                    break;
                case ExpressionOperators.FOR:
                    For f = (For) expression;
                    recursiveForCall(rdbmsContext, op, expressionList, f);
                    break;
                case ExpressionOperators.FOREACH:
                    ForEach e = (ForEach) expression;
                    recursiveForEachCall(rdbmsContext, op, expressionList, e);
                    break;
                case ExpressionOperators.STATEMENT:
                    Statement s = (Statement) expression;
                    recursiveStatementCall(rdbmsContext, op, expressionList, s);
                    break;
                default:
                    throw new IllegalStateException("New implementation of Expression not present in ExpressionOperators");
            }
        }
    }

    private static void recursiveConditionalCall(RDBMSContext rdbmsContext, Operation op, List<Expression> expressionList, Conditional c) {
        if (evaluateConditions(c.getIf(), op.getBindings(), rdbmsContext)) {
            recursiveThenCall(rdbmsContext, op, expressionList, c.getThen());
        } else {
            boolean notEnter = true;
            if (c.getElseIfList() != null && !c.getElseIfList().isEmpty()) {
                for (ElseIf ef : c.getElseIfList()) {
                    if (evaluateConditions(ef.getIf(), op.getBindings(), rdbmsContext)) {
                        notEnter = false;
                        recursiveThenCall(rdbmsContext, op, expressionList, ef.getThen());
                    }
                }
            }
            if (notEnter && c.getElse() != null) {
                recursiveThenCall(rdbmsContext, op, expressionList, c.getElse());
            }
        }
    }

    private static void recursiveForCall(RDBMSContext rdbmsContext, Operation op, List<Expression> expressionList, For f) {
        String var = f.getLoopCounterVariableName();
        Column temp = Column.createTemp(var, Integer.class.getCanonicalName());
        rdbmsContext.getOutVar().put(0, Integer.class, temp);
        int loopTimes = f.getLoopTimes();
        for (int i = 0; i < loopTimes; i++) {
            recursiveExpressionCall(rdbmsContext, op, f.getExpressions());
            rdbmsContext.getOutVar().update(i + 1, temp);
        }
    }

    private static void recursiveForEachCall( RDBMSContext rdbmsContext, Operation op, List<Expression> expressionList, ForEach e) {
        Path field = e.getIterateOverField();


        List values = null;
        String key = field.toString();
        String var = key+"Temp";
        if(!rdbmsContext.getInVar().getValues(key).isEmpty()){
            values = rdbmsContext.getInVar().getValues(key);
        } else {
            values = rdbmsContext.getOutVar().getValues(key);
        }

        if(values == null || values.isEmpty()) {
            return;
        }

        Column temp = Column.createTemp(var, values.get(0).getClass().getCanonicalName());
        rdbmsContext.getOutVar().put(values.get(0), values.get(0).getClass(), temp);
        for (int i = 0; i < values.size(); i++) {
            recursiveExpressionCall(rdbmsContext, op, e.getExpressions());
            if(i+1 < values.size()) {
                rdbmsContext.getOutVar().update(values.get(i + 1), temp);
            }
        }
    }

    private static void recursiveStatementCall(RDBMSContext rdbmsContext, Operation op, List<Expression> expressionList, Statement s) {
        String type = s.getType();
        String sql = s.getSQL();
        rdbmsContext.setSql(sql);
        rdbmsContext.setType(type);
        new ExecuteUpdateCommand(rdbmsContext).execute();
    }

    private static void recursiveThenCall( RDBMSContext rdbmsContext, Operation op, List<Expression> expressionList, Then then) {
        if (then.getExpressions() != null && !then.getExpressions().isEmpty()) {
            recursiveExpressionCall(rdbmsContext, op, then.getExpressions());
        } else {
            // "$fail", "$continue", "$break"
            // TODO put the flang into the context and make the static methods aware of it
            then.getLoopOperator();
        }
    }

    static boolean evaluateConditions(If i, Bindings bindings,RDBMSContext rdbmsContext) {
        final String simpleName = i.getClass().getSimpleName();
        final boolean allConditions;
        switch (simpleName) {
            case IfOperators.IFAND:
                allConditions = true;
                for (Object o : i.getConditions()) {
                    if (!evaluateConditions((If) o, bindings, rdbmsContext)) {
                        return false;
                    }
                }
                break;
            case IfOperators.IFFIELDCHECKFIELD:
            case IfOperators.IFFIELDCHECKVALUE:
            case IfOperators.IFFIELDCHECKVALUES:
            case IfOperators.IFFIELDEMPTY:
            case IfOperators.IFFIELDREGEX:
                return evaluateField(i, bindings, simpleName, rdbmsContext);
            case IfOperators.IFNOT:
                return !evaluateConditions((If) i.getConditions().get(0), bindings, rdbmsContext);
            case IfOperators.IFOR:
                allConditions = false;
                for (Object o : i.getConditions()) {
                    if (evaluateConditions((If) o, bindings, rdbmsContext)) {
                        return true;
                    }
                }
                break;
            default:
                throw new IllegalStateException("New implementation of If not present in IfOperators");
        }
        return allConditions;
    }

    private static boolean evaluateField(If i, Bindings bindings, String simpleName,RDBMSContext rdbmsContext) {
        switch (simpleName) {
            case IfOperators.IFFIELDCHECKFIELD:
                IfFieldCheckField fcf = (IfFieldCheckField) i;
                return ConditionalEvaluator.evaluate(rdbmsContext.getInputMappedByField().get(fcf.getField().toString()),fcf.getOp(),rdbmsContext.getInputMappedByField().get(fcf.getRfield().toString()),rdbmsContext);
            case IfOperators.IFFIELDCHECKVALUE:
                IfFieldCheckValue fcv = (IfFieldCheckValue) i;
                return ConditionalEvaluator.evaluate(rdbmsContext.getInputMappedByField().get(fcv.getField().toString()).toString(),fcv.getOp(),fcv.getValue(),rdbmsContext);
            case IfOperators.IFFIELDCHECKVALUES:
                IfFieldCheckValues fcs = (IfFieldCheckValues) i;
                return ConditionalEvaluator.evaluate(rdbmsContext.getInputMappedByField().get(fcs.getField().toString()),fcs.getOp(),fcs.getValues(),rdbmsContext);
            case IfOperators.IFFIELDEMPTY:
                IfFieldEmpty fe = (IfFieldEmpty) i;
                return ConditionalEvaluator.evaluateEmpty(fe, rdbmsContext);
            case IfOperators.IFFIELDREGEX:
                IfFieldRegex fr = (IfFieldRegex) i;
                return ConditionalEvaluator.evaluateRegex(fr, rdbmsContext);
            default:
                throw new IllegalStateException("New implementation of If not present in IfOperators");
        }
    }
}
