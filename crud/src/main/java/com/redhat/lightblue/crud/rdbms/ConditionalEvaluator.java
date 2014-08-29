package com.redhat.lightblue.crud.rdbms;

import com.redhat.lightblue.metadata.rdbms.converter.RDBMSContext;
import com.redhat.lightblue.metadata.rdbms.enums.OpOperators;
import com.redhat.lightblue.metadata.rdbms.model.IfFieldEmpty;
import com.redhat.lightblue.metadata.rdbms.model.IfFieldRegex;

import java.util.Collection;
import java.util.regex.Pattern;

/**
 * Created by lcestari on 8/26/14.
 */
public class ConditionalEvaluator {
    public static boolean evaluate(Object o, String op, Object o1, RDBMSContext rdbmsContext) {
        switch(op){
            case OpOperators.EQ:
                return o.equals(o1);
            case OpOperators.NEQ:
                return !o.equals(o1);
            case OpOperators.GT:
                int gt = ((Comparable) o).compareTo(o1);
                return 0 < gt? true:false;
            case OpOperators.GTE:
                int gte = ((Comparable) o).compareTo(o1);
                return gte == 0? true: 0 < gte? true:false;
            case OpOperators.LT:
                int lt = ((Comparable) o).compareTo(o1);
                return lt < 0? true:false;
            case OpOperators.LTE:
                int lte = ((Comparable) o).compareTo(o1);
                return lte == 0? true: lte < 0? true:false;
            case OpOperators.IN:
                return ((Collection) o1).contains(o);
            case OpOperators.NIN:
                return !((Collection) o1).contains(o);
            default:
                throw new IllegalStateException("Operation not found: "+op);
        }
    }

    public static boolean evaluateEmpty(IfFieldEmpty fe, RDBMSContext rdbmsContext) {
        return ((Collection)fe.getField()).isEmpty();
    }

    public static boolean evaluateRegex(IfFieldRegex fr, RDBMSContext rdbmsContext) {
        int flags = 0;
        if (fr.isCaseInsensitive()) {
            flags |= Pattern.CASE_INSENSITIVE;
        }
        if (fr.isMultiline()) {
            flags |= Pattern.MULTILINE;
        }
        if (fr.isExtended()) {
            flags |= Pattern.COMMENTS;
        }
        if (fr.isDotall()) {
            flags |= Pattern.DOTALL;
        }
        Pattern regex = Pattern.compile(fr.getRegex(), flags);
        return regex.matcher(fr.getField().toString()).matches();
    }
}
