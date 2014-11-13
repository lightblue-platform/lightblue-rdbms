package com.redhat.lightblue.crud.rdbms;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

import com.redhat.lightblue.metadata.rdbms.enums.OpOperators;
import com.redhat.lightblue.metadata.rdbms.model.IfFieldEmpty;
import com.redhat.lightblue.metadata.rdbms.model.IfFieldRegex;
import com.redhat.lightblue.util.Path;

public class ConditionalEvaluatorTest {

    @Test
    public void testEvaluate_EQ_True(){
        assertTrue(ConditionalEvaluator.evaluate("hello", OpOperators.EQ, "hello", null));
    }

    @Test
    public void testEvaluate_EQ_False(){
        assertFalse(ConditionalEvaluator.evaluate("hello", OpOperators.EQ, "goodbye", null));
    }

    @Test
    public void testEvaluate_NEQ_True(){
        assertTrue(ConditionalEvaluator.evaluate("hello", OpOperators.NEQ, "goodbye", null));
    }

    @Test
    public void testEvaluate_NEQ_False(){
        assertFalse(ConditionalEvaluator.evaluate("hello", OpOperators.NEQ, "hello", null));
    }

    @Test
    public void testEvaluate_GT_True(){
        assertTrue(ConditionalEvaluator.evaluate(4, OpOperators.GT, 3, null));
    }

    @Test
    public void testEvaluate_GT_False(){
        assertFalse(ConditionalEvaluator.evaluate(3, OpOperators.GT, 4, null));
    }

    @Test
    public void testEvaluate_GT_False_ValuesEqual(){
        assertFalse(ConditionalEvaluator.evaluate(3, OpOperators.GT, 3, null));
    }

    @Test
    public void testEvaluate_GTE_True(){
        assertTrue(ConditionalEvaluator.evaluate(4, OpOperators.GTE, 3, null));
    }

    @Test
    public void testEvaluate_GTE_False(){
        assertFalse(ConditionalEvaluator.evaluate(3, OpOperators.GTE, 4, null));
    }

    @Test
    public void testEvaluate_GTE_True_ValuesEqual(){
        assertTrue(ConditionalEvaluator.evaluate(3, OpOperators.GTE, 3, null));
    }

    @Test
    public void testEvaluate_LT_True(){
        assertTrue(ConditionalEvaluator.evaluate(3, OpOperators.LT, 4, null));
    }

    @Test
    public void testEvaluate_LT_False(){
        assertFalse(ConditionalEvaluator.evaluate(4, OpOperators.LT, 3, null));
    }

    @Test
    public void testEvaluate_LT_False_ValuesEqual(){
        assertFalse(ConditionalEvaluator.evaluate(3, OpOperators.LT, 3, null));
    }

    @Test
    public void testEvaluate_LTE_True(){
        assertTrue(ConditionalEvaluator.evaluate(3, OpOperators.LTE, 4, null));
    }

    @Test
    public void testEvaluate_LTE_False(){
        assertFalse(ConditionalEvaluator.evaluate(4, OpOperators.LTE, 3, null));
    }

    @Test
    public void testEvaluate_LTE_False_ValuesEqual(){
        assertTrue(ConditionalEvaluator.evaluate(3, OpOperators.LTE, 3, null));
    }

    @Test
    public void testEvaluate_IN_True(){
        assertTrue(ConditionalEvaluator.evaluate(3, OpOperators.IN, Arrays.asList(3), null));
    }

    @Test
    public void testEvaluate_IN_False(){
        assertFalse(ConditionalEvaluator.evaluate(4, OpOperators.IN, Arrays.asList(3), null));
    }

    @Test
    public void testEvaluate_NIN_True(){
        assertTrue(ConditionalEvaluator.evaluate(4, OpOperators.NIN, Arrays.asList(3), null));
    }

    @Test
    public void testEvaluate_NIN_False(){
        assertFalse(ConditionalEvaluator.evaluate(3, OpOperators.NIN, Arrays.asList(3), null));
    }

    @Test(expected = IllegalStateException.class)
    public void testEvaluate_UnknownOperator(){
        ConditionalEvaluator.evaluate("some value", "fake operator", "some value", null);
    }

    @Test
    public void testEvaluateEmpty_True(){
        IfFieldEmpty fe = new IfFieldEmpty();
        fe.setField(new Path());
        assertTrue(ConditionalEvaluator.evaluateEmpty(fe, null));
    }

    @Test
    public void testEvaluateEmpty_False(){
        IfFieldEmpty fe = new IfFieldEmpty();
        fe.setField(new Path("somevalue"));
        assertFalse(ConditionalEvaluator.evaluateEmpty(fe, null));
    }

    @Test
    public void testEvaluateRegex_NoFlags_Matches(){
        IfFieldRegex fr = new IfFieldRegex();
        fr.setRegex("some\\w*e");
        fr.setField(new Path("somevalue"));
        assertTrue(ConditionalEvaluator.evaluateRegex(fr, null));
    }

    @Test
    public void testEvaluateRegex_NoFlags_DoesNotMatch(){
        IfFieldRegex fr = new IfFieldRegex();
        fr.setRegex("XXXXsome\\w*e");
        fr.setField(new Path("somevalue"));
        assertFalse(ConditionalEvaluator.evaluateRegex(fr, null));
    }

}
