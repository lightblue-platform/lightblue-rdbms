package com.redhat.lightblue.crud.rdbms;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.redhat.lightblue.crud.CRUDOperationContext;
import com.redhat.lightblue.crud.DocCtx;
import com.redhat.lightblue.crud.Factory;
import com.redhat.lightblue.crud.Operation;
import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.metadata.rdbms.converter.DynVar;
import com.redhat.lightblue.metadata.rdbms.converter.RDBMSContext;
import com.redhat.lightblue.metadata.rdbms.enums.OpOperators;
import com.redhat.lightblue.metadata.rdbms.model.Bindings;
import com.redhat.lightblue.metadata.rdbms.model.IfFieldCheckField;
import com.redhat.lightblue.metadata.rdbms.model.RDBMS;
import com.redhat.lightblue.metadata.rdbms.util.Column;
import com.redhat.lightblue.util.JsonDoc;
import com.redhat.lightblue.util.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;

public class RDBMSProcessorTest {
    RDBMSContext rdbmsContext;
    static String entityName = "entityName";

    @Before
    public void setUp() throws Exception {
        rdbmsContext = new RDBMSContext();
        final EntityMetadata entityMetadata = new EntityMetadata(entityName);
        rdbmsContext.setEntityMetadata(entityMetadata);
        rdbmsContext.getEntityMetadata().getEntitySchema().getProperties().put("rdbms", new RDBMS());
        rdbmsContext.setRDBMSDataSourceResolver(new MyRDBMSDataSourceResolver());
        rdbmsContext.setCRUDOperationName("delete");
        CRUDOperationContext c = new CRUDOperationContext(Operation.INSERT, entityName, new Factory(), new ArrayList<JsonDoc>()) {
            @Override
            public EntityMetadata getEntityMetadata(String s) {
                return entityMetadata;
            }
        };
        rdbmsContext.setCrudOperationContext(c);
    }

    @Test
    public void testProcess() throws Exception {
        RDBMSProcessor.process(rdbmsContext);
    }

    @Test
    public void testEvaluateConditions() throws Exception {
        IfFieldCheckField i = new IfFieldCheckField();
        Path fie = new Path("fie");
        i.setField(fie);
        i.setRfield(fie);
        i.setOp(OpOperators.EQ);
        rdbmsContext.setInputMappedByField(new HashMap<String, Object>());
        rdbmsContext.getInputMappedByField().put(i.getField().toString(), i.getField());
        rdbmsContext.getInputMappedByField().put(i.getRfield().toString(), i.getRfield());

        RDBMSProcessor.evaluateConditions(i,new Bindings(),rdbmsContext);
    }

    @Test
    public void testConvertProjectionSingleNode() throws Exception {
        rdbmsContext.setJsonNodeFactory(JsonNodeFactory.instance);
        assertNotNull(rdbmsContext.getJsonNodeFactory());
        assertNotNull(rdbmsContext.getCrudOperationContext());

        final Integer value = new Integer(128);
        final Class<?> objectClass = Integer.class;
        DynVar dynVar = new DynVar(rdbmsContext);
        String path = "parameter";
        Column column = new Column(0, path, path, "java.lang.Boolean", Types.INTEGER);
        dynVar.put(value, objectClass, column);
        RDBMSProcessor.convertProjection(rdbmsContext,null, dynVar);

        JsonDoc jd = new JsonDoc(new ObjectNode(rdbmsContext.getJsonNodeFactory()));
        jd.modify(new Path(path),new TextNode(value.toString()),true);
        DocCtx expected =  new DocCtx(jd);

        List<DocCtx> documents = rdbmsContext.getCrudOperationContext().getDocuments();
        assertEquals(1, documents.size());
        //{"parameter":"256"}
        assertEquals(expected.getRoot(), documents.get(0).getOutputDocument().getRoot());
    }


    @Test
    public void testConvertProjectionArrayNode() throws Exception {
        rdbmsContext.setJsonNodeFactory(JsonNodeFactory.instance);
        assertNotNull(rdbmsContext.getJsonNodeFactory());
        assertNotNull(rdbmsContext.getCrudOperationContext());

        final Integer value1 = new Integer(512);
        final Integer value2 = new Integer(2);
        final Class<?> objectClass = Integer.class;
        DynVar dynVar = new DynVar(rdbmsContext);
        String path = "parameter";
        Column column = new Column(0, path, path, "java.lang.Boolean", Types.INTEGER);
        dynVar.put(value1, objectClass, column);
        dynVar.put(value2, objectClass, column);
        RDBMSProcessor.convertProjection(rdbmsContext,null, dynVar);

        JsonDoc jd = new JsonDoc(new ObjectNode(rdbmsContext.getJsonNodeFactory()));
        ArrayNode arrayNode = new ArrayNode(rdbmsContext.getJsonNodeFactory());
        arrayNode.add(value1.toString());
        arrayNode.add(value2.toString());
        jd.modify(new Path(path), arrayNode, true);
        DocCtx expected =  new DocCtx(jd);

        List<DocCtx> documents = rdbmsContext.getCrudOperationContext().getDocuments();
        assertEquals(1, documents.size());
        //{"parameter":["512","2"]}
        assertEquals(expected.getRoot(), documents.get(0).getOutputDocument().getRoot());
    }
}