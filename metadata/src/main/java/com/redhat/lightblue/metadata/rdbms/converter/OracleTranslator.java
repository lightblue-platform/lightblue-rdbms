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
import com.redhat.lightblue.metadata.rdbms.model.Join;
import com.redhat.lightblue.metadata.rdbms.model.ProjectionMapping;
import com.redhat.lightblue.query.RegexMatchExpression;
import com.redhat.lightblue.util.*;
import com.redhat.lightblue.util.Error;

import java.util.LinkedList;

/**
 *
 * Translator based on Oracle 10.0+
 * @author lcestari
 */
public class OracleTranslator extends Translator {

    @Override
    protected void generateWhere(SelectStmt selectStmt, StringBuilder queryStringBuilder, LinkedList<String> whereConditionals) {
        super.generateWhere(selectStmt, queryStringBuilder,whereConditionals);
        Long limit = selectStmt.getRange().getLimit();
        Long offset = selectStmt.getRange().getOffset();
        if (limit != null && offset != null) {
            offset = offset +limit;
            queryStringBuilder.append("AND ROWNUM BETWEEN ").append(Long.toString(offset)).append(" AND ").append(Long.toString(limit)).append(" ");
        } else if (limit != null) {
            queryStringBuilder.append("AND ROWNUM >= ").append(Long.toString(limit)).append(" ");
        } else if (offset != null) {
            queryStringBuilder.append("AND ROWNUM <=").append(Long.toString(offset)).append(" ");
        }
    }

    @Override
    protected void generateLimitOffset(SelectStmt selectStmt, StringBuilder queryStringBuilder, Range range) {
        // Stop the default implementation to run
    }

    @Override
    protected void recursiveTranslateRegexMatchExpression(TranslationContext translationContext, RegexMatchExpression regexMatchExpression) {
        String regex = regexMatchExpression.getRegex();
        Path lField = regexMatchExpression.getField();

        String f = lField.toString();

        ProjectionMapping fpm = translationContext.fieldToProjectionMap.get(f);
        Join fJoin = translationContext.projectionToJoinMap.get(fpm);
        fillTables(translationContext, translationContext.baseStmt.getFromTables(), fJoin);
        fillWhere(translationContext, translationContext.baseStmt.getWhereConditionals(), fJoin);

        if(translationContext.notOp){
            throw Error.get(RDBMSConstants.ERR_NO_OPERATOR, regexMatchExpression.toString());
        }
        String options = regexMatchExpression.isCaseInsensitive()?"i":"c";
        options = options + (regexMatchExpression.isDotAll()?"n":"");
        options = options + (regexMatchExpression.isMultiline()?"m":"");
        String s =  "REGEXP_LIKE("+ fpm.getColumn() +",'"+ regex + "','"+ options +"')";
        addConditional(translationContext, s);
    }
}
