package com.redhat.lightblue.metadata.rdbms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.redhat.lightblue.crud.CRUDOperationContext;
import com.redhat.lightblue.crud.Factory;
import com.redhat.lightblue.crud.Operation;
import com.redhat.lightblue.metadata.ArrayField;
import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.metadata.SimpleArrayElement;
import com.redhat.lightblue.metadata.SimpleField;
import com.redhat.lightblue.metadata.rdbms.converter.RDBMSContext;
import com.redhat.lightblue.metadata.rdbms.converter.SelectStmt;
import com.redhat.lightblue.metadata.rdbms.converter.Translator;
import com.redhat.lightblue.metadata.rdbms.model.ColumnToField;
import com.redhat.lightblue.metadata.rdbms.model.Join;
import com.redhat.lightblue.metadata.rdbms.model.ProjectionMapping;
import com.redhat.lightblue.metadata.rdbms.model.RDBMS;
import com.redhat.lightblue.metadata.rdbms.model.SQLMapping;
import com.redhat.lightblue.metadata.rdbms.model.Table;
import com.redhat.lightblue.metadata.types.StringType;
import com.redhat.lightblue.query.FieldProjection;
import com.redhat.lightblue.query.QueryExpression;
import com.redhat.lightblue.util.JsonDoc;
import com.redhat.lightblue.util.JsonUtils;
import com.redhat.lightblue.util.Path;

public class TranslatorTest {
    final String valueQuery1 = "{\"field\":\"x.\", \"op\":\"$eq\", \"rvalue\":\"stringXPTO\"}";
    final String fieldQuery1 = "{\"field\":\"x\", \"op\":\"$eq\", \"rfield\":\"y\"}";
    final String naryQuery1 = "{\"field\":\"x\", \"op\":\"$in\", \"values\":[1,2,3,4,5]}";
    final String regexQuery1 = "{\"field\":\"x\", \"regex\":\"*pat*\"}";
    final String unaryQuery1 = "{ \"$not\": " + valueQuery1 + "}";
    final String naryLogicalQuery1 = "{ \"$or\" : [" + valueQuery1 + "," + fieldQuery1 + "," + naryQuery1 + "," + unaryQuery1 + "]}";
    final String arrContains1 = "{\"array\":\"z\", \"contains\":\"$any\", \"values\":[1,2,3,4,5]}";
    final String arrContains2 = "{\"array\":\"z\", \"contains\":\"$all\", \"values\":[1,2,3,4,5]}";

    Translator cut = Translator.ORACLE;
    CRUDOperationContext crud = new TestCRUD();
    RDBMSContext rdbmsContext = new RDBMSContext();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        crud = new TestCRUD();
        rdbmsContext = new RDBMSContext();
        EntityMetadata em = new EntityMetadata("test");
        em.getEntitySchema().getFields().addNew(new SimpleField("x", StringType.TYPE));
        em.getEntitySchema().getFields().addNew(new ArrayField("z", new SimpleArrayElement(StringType.TYPE)));
        rdbmsContext.setEntityMetadata(em);
        RDBMS rdbms = new RDBMS();
        SQLMapping sQLMapping = new SQLMapping();
        ArrayList<Join> joins = new ArrayList<Join>();
        Join e = new Join();
        Table table = new Table();
        table.setName("123");
        table.setAlias("xyz");
        e.getTables().add(table);
        table = new Table();
        table.setName("K");
        table.setAlias("w");
        e.getTables().add(table);
        e.setJoinTablesStatement(" xyz.c1 = w.c2 ");
        ProjectionMapping projectionMapping = new ProjectionMapping();
        projectionMapping.setColumn("w.z1");
        projectionMapping.setField("z");
        e.getProjectionMappings().add(projectionMapping);
        projectionMapping = new ProjectionMapping();
        projectionMapping.setColumn("xyz.x1");
        projectionMapping.setField("x");
        e.getProjectionMappings().add(projectionMapping);
        joins.add(e);
        e = new Join();
        table = new Table();
        table.setName("YYYY");
        table.setAlias("YyYy");
        e.getTables().add(table);
        //NO e.setJoinTablesStatement
        projectionMapping = new ProjectionMapping();
        projectionMapping.setColumn("YyYy.y");
        projectionMapping.setField("y");
        e.getProjectionMappings().add(projectionMapping);
        joins.add(e);
        sQLMapping.setJoins(joins);
        sQLMapping.setColumnToFieldMap(new ArrayList<ColumnToField>());
        rdbms.setSQLMapping(sQLMapping);
        rdbmsContext.setProjection(new FieldProjection(new Path("x"), true, true));
        rdbmsContext.setRdbms(rdbms);
        rdbmsContext.setCrudOperationContext(crud);
    }

    @After
    public void tearDown() throws Exception {
        crud = null;
        rdbmsContext = null;
    }

    @Test
    public void testNoQuery() throws Exception {
        exception.expect(com.redhat.lightblue.util.Error.class);
        String s = "{\"objectType\":\"error\",\"context\":\"translateQuery\",\"errorCode\":\"rdbms:NoSupportedQuery\",\"msg\":\"q=null\"}";
        exception.expectMessage(JsonUtils.json(s).toString());
        cut.translate(rdbmsContext);
    }


    @Test
    public void testArrayContainsAny() throws Exception {
        exception.expect(com.redhat.lightblue.util.Error.class);
        exception.expectMessage("{\"objectType\":\"error\",\"context\":\"translateQuery\",\"errorCode\":\"rdbms:NoSupportedOperator\",\"msg\":\"{\\\"array\\\":\\\"z\\\",\\\"contains\\\":\\\"$any\\\",\\\"values\\\":[1,2,3,4,5]}\"}");
        rdbmsContext.setQueryExpression(generateQuery(arrContains1));
        cut.translate(rdbmsContext);

    }

    @Test
    public void testArrayContainsAll() throws Exception {
        rdbmsContext.setQueryExpression(generateQuery(arrContains2));
        List<SelectStmt> translate = cut.translate(rdbmsContext);
        Assert.assertNotNull(translate);
        Assert.assertTrue("translate size is different than 1", translate.size() == 1);
        String result = translate.get(0).generateStatement();
        // test any order of select statement 's selected columns
        String extectedHead1 = "SELECT xyz.x1 ,w.z1 ";
        String extectedHead2 = "SELECT w.x1 ,xyz.z1 ";
        String expectedTail = "FROM 123 AS xyz ,K AS w WHERE  xyz.c1 = w.c2  AND w.z1 IN ('1','2','3','4','5') ";
        String comb1 =  extectedHead1 + expectedTail;
        String comb2 =  extectedHead2 + expectedTail;
        if(!comb1.equals(result) && !comb2.equals(result) ) {
            String expected = comb1;
            Assert.assertEquals(expected, result);
        }

    }

    @Test
    public void testValue() throws Exception {
        rdbmsContext.setQueryExpression(generateQuery(valueQuery1));
        List<SelectStmt> translate = cut.translate(rdbmsContext);
        Assert.assertNotNull(translate);
        Assert.assertTrue("translate size is different than 1", translate.size() == 1);
        String expected = "SELECT xyz.x1 FROM 123 AS xyz ,K AS w WHERE  xyz.c1 = w.c2  AND xyz.x1 = 'stringXPTO' ";
        Assert.assertEquals(expected,translate.get(0).generateStatement());
    }

    @Test
    public void testField() throws Exception {
        rdbmsContext.setQueryExpression(generateQuery(fieldQuery1));
        List<SelectStmt> translate = cut.translate(rdbmsContext);
        Assert.assertNotNull(translate);
        Assert.assertTrue("translate size is different than 1", translate.size() == 1);
        String expected = "SELECT xyz.x1 FROM 123 AS xyz ,K AS w ,YYYY AS YyYy WHERE  xyz.c1 = w.c2  AND xyz.x1 = YyYy.y ";
        Assert.assertEquals(expected,translate.get(0).generateStatement());
    }

    @Test
    public void testNary() throws Exception {
        rdbmsContext.setQueryExpression(generateQuery(naryQuery1));
        List<SelectStmt> translate = cut.translate(rdbmsContext);
        Assert.assertNotNull(translate);
        Assert.assertTrue("translate size is different than 1", translate.size() == 1);
        String expected = "SELECT xyz.x1 FROM 123 AS xyz ,K AS w WHERE  xyz.c1 = w.c2  AND xyz.x1 IN ('1','2','3','4','5') ";
        Assert.assertEquals(expected,translate.get(0).generateStatement());
    }

    @Test
    public void testRegex() throws Exception {
        rdbmsContext.setQueryExpression(generateQuery(regexQuery1));
        List<SelectStmt> translate = cut.translate(rdbmsContext);
        Assert.assertNotNull(translate);
        Assert.assertTrue("translate size is different than 1", translate.size() == 1);
        String expected = "SELECT xyz.x1 FROM 123 AS xyz ,K AS w WHERE  xyz.c1 = w.c2  AND REGEXP_LIKE(xyz.x1,'*pat*','c') ";
        Assert.assertEquals(expected,translate.get(0).generateStatement());
    }

    @Test
    public void testUnary() throws Exception {
        rdbmsContext.setQueryExpression(generateQuery(unaryQuery1));
        List<SelectStmt> translate = cut.translate(rdbmsContext);
        Assert.assertNotNull(translate);
        Assert.assertTrue("translate size is different than 1", translate.size() == 1);
        String expected = "SELECT xyz.x1 FROM 123 AS xyz ,K AS w WHERE  xyz.c1 = w.c2  AND xyz.x1 <> 'stringXPTO' ";
        Assert.assertEquals(expected,translate.get(0).generateStatement());
    }

    @Test
    public void testNaryLogical() throws Exception {
        rdbmsContext.setQueryExpression(generateQuery(naryLogicalQuery1));
        List<SelectStmt> translate = cut.translate(rdbmsContext);
        Assert.assertNotNull(translate);
        Assert.assertTrue("translate size is different than 1", translate.size() == 1);
        String expected = "SELECT xyz.x1 FROM 123 AS xyz ,K AS w ,123 AS xyz ,K AS w ,YYYY AS YyYy ,123 AS xyz ,K AS w ,123 AS xyz ,K AS w WHERE  xyz.c1 = w.c2  AND  xyz.c1 = w.c2  AND  xyz.c1 = w.c2  AND  xyz.c1 = w.c2  AND (xyz.x1 = 'stringXPTO' or xyz.x1 = YyYy.y or xyz.x1 IN ('1','2','3','4','5') or xyz.x1 <> 'stringXPTO')  ";
        Assert.assertEquals(expected,translate.get(0).generateStatement());
    }

    static class TestCRUD extends CRUDOperationContext {
        public TestCRUD(){
            this(null, null, new Factory(), null);
        }
        public TestCRUD(Operation op, String entityName, Factory f, List<JsonDoc> docs) {
            super(op, entityName, f, docs);
        }
        @Override public EntityMetadata getEntityMetadata(String entityName) {
            return null;
        }
    }

    private QueryExpression generateQuery(String str) throws IOException {
        return QueryExpression.fromJson(JsonUtils.json(str));
    }
}
