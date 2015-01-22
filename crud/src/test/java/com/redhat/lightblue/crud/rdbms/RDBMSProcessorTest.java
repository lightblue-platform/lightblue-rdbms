package com.redhat.lightblue.crud.rdbms;

import com.redhat.lightblue.crud.CRUDOperationContext;
import com.redhat.lightblue.crud.Factory;
import com.redhat.lightblue.crud.Operation;
import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.metadata.rdbms.converter.RDBMSContext;
import com.redhat.lightblue.metadata.rdbms.enums.OpOperators;
import com.redhat.lightblue.metadata.rdbms.model.Bindings;
import com.redhat.lightblue.metadata.rdbms.model.IfFieldCheckField;
import com.redhat.lightblue.metadata.rdbms.model.RDBMS;
import com.redhat.lightblue.util.JsonDoc;
import com.redhat.lightblue.util.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;

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
}