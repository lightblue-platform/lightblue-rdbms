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
package com.redhat.lightblue.metadata.rdbms.impl;

import java.util.ArrayList;
import java.util.List;

import com.redhat.lightblue.common.rdbms.RDBMSConstants;
import com.redhat.lightblue.metadata.parser.MetadataParser;
import com.redhat.lightblue.metadata.parser.PropertyParser;
import com.redhat.lightblue.metadata.rdbms.enums.LightblueOperators;
import com.redhat.lightblue.metadata.rdbms.model.Bindings;
import com.redhat.lightblue.metadata.rdbms.model.Conditional;
import com.redhat.lightblue.metadata.rdbms.model.Else;
import com.redhat.lightblue.metadata.rdbms.model.ElseIf;
import com.redhat.lightblue.metadata.rdbms.model.Expression;
import com.redhat.lightblue.metadata.rdbms.model.For;
import com.redhat.lightblue.metadata.rdbms.model.ForEach;
import com.redhat.lightblue.metadata.rdbms.model.If;
import com.redhat.lightblue.metadata.rdbms.model.InOut;
import com.redhat.lightblue.metadata.rdbms.model.Operation;
import com.redhat.lightblue.metadata.rdbms.model.RDBMS;
import com.redhat.lightblue.metadata.rdbms.model.SQLMapping;
import com.redhat.lightblue.metadata.rdbms.model.Statement;
import com.redhat.lightblue.metadata.rdbms.model.Then;
import com.redhat.lightblue.util.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RDBMSPropertyParserImpl<T> extends PropertyParser<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RDBMSPropertyParserImpl.class);
    public static final String NAME = "rdbms";

    @Override
    public RDBMS parse(String name, MetadataParser<T> p, T node) {
        if (!NAME.equals(name)) {
            throw com.redhat.lightblue.util.Error.get(RDBMSConstants.ERR_WRONG_ROOT_NODE_NAME, "Node name informed:" + name);
        }

        String dialect = p.getStringProperty(node, "dialect");
        if (dialect == null || dialect.isEmpty()) {
            throw com.redhat.lightblue.util.Error.get(RDBMSConstants.ERR_FIELD_REQUIRED, "No field informed");
        }

        T delete = p.getObjectProperty(node, LightblueOperators.DELETE);
        T fetch = p.getObjectProperty(node, LightblueOperators.FETCH);
        T insert = p.getObjectProperty(node, LightblueOperators.INSERT);
        T save = p.getObjectProperty(node, LightblueOperators.SAVE);
        T update = p.getObjectProperty(node, LightblueOperators.UPDATE);

        if (delete == null && fetch == null && insert == null && save == null && update == null) {
            throw com.redhat.lightblue.util.Error.get(RDBMSConstants.ERR_FIELD_REQUIRED, "No Operation informed");
        }

        RDBMS rdbms = new RDBMS();
        rdbms.setDialect(dialect);
        rdbms.setDelete(parseOperation(p, delete, LightblueOperators.DELETE));
        rdbms.setFetch(parseOperation(p, fetch, LightblueOperators.FETCH));
        rdbms.setInsert(parseOperation(p, insert, LightblueOperators.INSERT));
        rdbms.setSave(parseOperation(p, save, LightblueOperators.SAVE));
        rdbms.setUpdate(parseOperation(p, update, LightblueOperators.UPDATE));

        SQLMapping s = new SQLMapping();
        s.parse(p, p.getObjectProperty(node, "SQLMapping"));
        rdbms.setSQLMapping(s);

        return rdbms;
    }

    @Override
    public void convert(MetadataParser<T> p, T parent, Object object) {
        if (object == null) {
            throw new IllegalArgumentException("No RDBMS object was informed!");
        }
        RDBMS rdbms = (RDBMS) object;
        rdbms.convert(p, parent);
    }

    private Operation parseOperation(MetadataParser<T> p, T operation, String fieldName) {
        if (operation == null) {
            return null;
        }
        T b = p.getObjectProperty(operation, "bindings");
        Bindings bindings = null;
        if (b != null) {
            bindings = parseBindings(p, b);
        }
        List<T> expressionsT = p.getObjectList(operation, "expressions");
        if (expressionsT == null || expressionsT.isEmpty()) {
            throw com.redhat.lightblue.util.Error.get(RDBMSConstants.ERR_FIELD_REQUIRED, "No expressions informed for Operation " + fieldName);
        }
        List<Expression> expressions = parseExpressions(p, expressionsT);

        final Operation s = new Operation();
        s.setBindings(bindings);
        s.setExpressionList(expressions);
        s.setName(fieldName);

        return s;
    }

    private Bindings parseBindings(MetadataParser<T> p, T bindings) {
        final Bindings b = new Bindings();
        List<T> inRaw = p.getObjectList(bindings, "in");
        List<T> outRaw = p.getObjectList(bindings, "out");
        boolean bI = inRaw == null || inRaw.isEmpty();
        boolean bO = outRaw == null || outRaw.isEmpty();
        if (bI && bO) {
            throw com.redhat.lightblue.util.Error.get(RDBMSConstants.ERR_FIELD_REQUIRED, "No InOut informed for Binding");
        }

        if (!bI) {
            List<InOut> inList = parseInOut(p, inRaw);
            b.setInList(inList);
        }
        if (!bO) {
            List<InOut> outList = parseInOut(p, outRaw);
            b.setOutList(outList);
        }

        return b;
    }

    private List<InOut> parseInOut(MetadataParser<T> p, List<T> inRaw) {
        final ArrayList<InOut> result = new ArrayList<>();
        for (T t : inRaw) {
            InOut a = new InOut();
            String column = p.getStringProperty(t, "column");
            if (column == null || column.isEmpty()) {
                throw com.redhat.lightblue.util.Error.get(RDBMSConstants.ERR_FIELD_REQUIRED, "No column informed");
            }
            a.setColumn(column);

            String path = p.getStringProperty(t, "field");
            if (path == null || path.isEmpty()) {
                throw com.redhat.lightblue.util.Error.get(RDBMSConstants.ERR_FIELD_REQUIRED, "No field informed");
            }
            a.setField(new Path(path));

            result.add(a);
        }
        return result;
    }

    private List<Expression> parseExpressions(MetadataParser<T> p, List<T> expressionsT) {
        final ArrayList<Expression> result = new ArrayList<>();
        for (T expression : expressionsT) {
            Expression e;
            T stmt = p.getObjectProperty(expression, "statement");
            T forS = p.getObjectProperty(expression, "for");
            T foreachS = p.getObjectProperty(expression, "foreach");
            T ifthen = p.getObjectProperty(expression, "if");

            if (stmt != null) {
                String sql = p.getStringProperty(stmt, "sql");
                String type = p.getStringProperty(stmt, "type");
                boolean sqlB = sql == null || sql.isEmpty();
                boolean typeB = type == null || type.isEmpty();
                if (sqlB || typeB) {
                    throw com.redhat.lightblue.util.Error.get(RDBMSConstants.ERR_FIELD_REQUIRED, "Invalid statement: No sql or type informed");
                }

                Statement statement = new Statement();
                statement.setSQL(sql);
                statement.setType(type);

                e = statement;
            } else if (forS != null) {
                String loopTimesS = p.getStringProperty(forS, "loopTimes");
                String loopCounterVariableName = p.getStringProperty(forS, "loopCounterVariableName");
                List<T> expressionsTforS = p.getObjectList(forS, "expressions");
                boolean loopTimesSB = loopTimesS == null || loopTimesS.isEmpty();
                boolean loopCounterVariableNameB = loopCounterVariableName == null || loopCounterVariableName.isEmpty();
                boolean expressionsTforSB = expressionsTforS == null || expressionsTforS.isEmpty();
                if (loopTimesSB || loopCounterVariableNameB || expressionsTforSB) {
                    throw com.redhat.lightblue.util.Error.get(RDBMSConstants.ERR_FIELD_REQUIRED, "Invalid for: No loopTimesS or loopCounterVariableName or expressions informed");
                }
                List<Expression> expressions = parseExpressions(p, expressionsTforS);
                int loopTimes = 0;
                try {
                    loopTimes = Integer.parseInt(loopTimesS);
                } catch (NumberFormatException nfe) {
                    throw com.redhat.lightblue.util.Error.get(RDBMSConstants.ERR_FIELD_REQUIRED, "Invalid for: loopTimes is not an integer");
                }
                For forLoop = new For();
                forLoop.setLoopTimes(loopTimes);
                forLoop.setLoopCounterVariableName(loopCounterVariableName);
                forLoop.setExpressions(expressions);

                e = forLoop;
            } else if (foreachS != null) {
                String iterateOverPath = p.getStringProperty(foreachS, "iterateOverField");
                List<T> expressionsTforS = p.getObjectList(foreachS, "expressions");
                boolean iterateOverPathB = iterateOverPath == null || iterateOverPath.isEmpty();
                boolean expressionsTforSB = expressionsTforS == null || expressionsTforS.isEmpty();
                if (iterateOverPathB || expressionsTforSB) {
                    throw com.redhat.lightblue.util.Error.get(RDBMSConstants.ERR_FIELD_REQUIRED, "Invalid foreach: No iterateOverField or expressions informed");
                }
                List<Expression> expressions = parseExpressions(p, expressionsTforS);

                ForEach forLoop = new ForEach();
                forLoop.setIterateOverField(new Path(iterateOverPath));
                forLoop.setExpressions(expressions);

                e = forLoop;
            } else if (ifthen != null) {
                //if
                If If = parseIf(p, ifthen);

                //then
                Then Then = parseThen(p, expression);

                //elseIf
                List<ElseIf> elseIfList = parseElseIf(p, expression);

                //else
                Else elseC = parseElse(p, expression);

                Conditional c = new Conditional();
                c.setIf(If);
                c.setThen(Then);
                c.setElseIfList(elseIfList);
                c.setElse(elseC);

                e = c;
            } else {
                throw com.redhat.lightblue.util.Error.get(RDBMSConstants.ERR_WRONG_FIELD, "No valid field was set as expression ->" + expression.toString());
            }
            result.add(e);
        }
        return result;
    }

    private List<ElseIf> parseElseIf(MetadataParser<T> p, T expression) {
        List<T> elseIfs = p.getObjectList(expression, "elseIf");

        if (elseIfs != null && !elseIfs.isEmpty()) {
            List<ElseIf> elseIfList = new ArrayList<>();
            for (T ei : elseIfs) {
                T eiIfT = p.getObjectProperty(ei, "if");
                T eiThenT = p.getObjectProperty(ei, "then");
                boolean ifB = eiIfT == null;
                boolean thenB = eiThenT == null;
                if (ifB || thenB) {
                    throw com.redhat.lightblue.util.Error.get(RDBMSConstants.ERR_FIELD_REQUIRED, "Invalid elseIf: No if or then informed");
                }
                If eiIf = parseIf(p, eiIfT);
                Then eiThen = parseThen(p, ei);

                ElseIf elseIf = new ElseIf();
                elseIf.setIf(eiIf);
                elseIf.setThen(eiThen);
                elseIfList.add(elseIf);
            }
            return elseIfList;
        } else if (elseIfs != null && elseIfs.isEmpty()) {
            throw com.redhat.lightblue.util.Error.get(RDBMSConstants.ERR_FIELD_REQUIRED, "Invalid elseIf: the elseIf array is empty");
        } else {
            return null;
        }
    }

    private If parseIf(MetadataParser<T> p, T ifT) {
        return (If) If.getChain().parse(p, ifT);
    }

    private Then parseThenOrElse(MetadataParser<T> p, T t, String name, Then then) {
        try {
            String loopOperator = p.getStringProperty(t, name); // getStringProperty  doesnt throw exception when field doesnt exist (but if it doesnt and it isnt the right type it throws and execption)
            if (loopOperator != null) {
                then.setLoopOperator(loopOperator);
            } else {
                List<T> expressionsT = p.getObjectList(t, name);
                List<Expression> expressions = parseExpressions(p, expressionsT);
                then.setExpressions(expressions);
            }
        } catch (com.redhat.lightblue.util.Error e) {
            List<T> expressionsT = p.getObjectList(t, name);
            List<Expression> expressions = parseExpressions(p, expressionsT);
            then.setExpressions(expressions);
        } catch (Exception te) {
            LOGGER.error("Expression returned an exception",te);

            return null;
        }

        return then;
    }

    private Then parseThen(MetadataParser<T> p, T parentThenT) {
        return parseThenOrElse(p, parentThenT, "then", new Then());
    }

    private Else parseElse(MetadataParser<T> p, T parentElseT) {
        return (Else) parseThenOrElse(p, parentElseT, "else", new Else());
    }
}
