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

import com.fasterxml.jackson.databind.JsonNode;
import com.redhat.lightblue.crud.CRUDOperationContext;
import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.crud.Operation;
import com.redhat.lightblue.metadata.rdbms.enums.DialectOperators;
import com.redhat.lightblue.metadata.rdbms.enums.LightblueOperators;
import com.redhat.lightblue.metadata.rdbms.model.*;
import com.redhat.lightblue.metadata.rdbms.model.Statement;
import com.redhat.lightblue.query.*;
import com.redhat.lightblue.util.JsonDoc;
import com.redhat.lightblue.util.Path;
import org.junit.Before;
import org.junit.Test;
import com.redhat.lightblue.crud.Factory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class RDBMSCRUDControllerTest {

    public static String entityName = "entityName";
    public static DataSource dsMock = null;
    public static Connection cMock = null;
    public static String statement = null;
    public static PreparedStatement psMock = null;

    public CRUDOperationContext c = null;
    public RDBMSCRUDController cut = null; //class under test


    public static final EntityMetadata entityMetadata = new EntityMetadata(entityName);
    static {
        String role = "anyone";
        entityMetadata.getAccess().getInsert().setRoles(role);
        entityMetadata.getAccess().getDelete().setRoles(role);
        entityMetadata.getAccess().getFind().setRoles(role);
        entityMetadata.getAccess().getUpdate().setRoles(role);

        entityMetadata.getEntitySchema().getProperties().put("rdbms", new RDBMS());
    }

    @Before
    public void setUp() throws Exception {
        cut = new RDBMSCRUDController(new MyRDBMSDataSourceResolver());

        c = new CRUDOperationContext(Operation.INSERT , entityName, new Factory() , new ArrayList<JsonDoc>()) {
            @Override
            public EntityMetadata getEntityMetadata(String s) {
                return entityMetadata;
            }
        };
        //basic rdbms
        RDBMS rdbms = (RDBMS) c.getEntityMetadata(entityName).getEntitySchema().getProperties().get("rdbms");
        com.redhat.lightblue.metadata.rdbms.model.Operation delete = new com.redhat.lightblue.metadata.rdbms.model.Operation();
        delete.setName(LightblueOperators.DELETE);
        ArrayList<Expression> expressionList = new ArrayList<>();
        Statement e = new Statement();
        e.setSQL("SQL");
        e.setType("delete");
        expressionList.add(e);
        delete.setExpressionList(expressionList);
        rdbms.setDelete(delete);
        rdbms.setDialect(DialectOperators.ORACLE);

        //rdbms details for this test
        SQLMapping sQLMapping = new SQLMapping();
        ArrayList<Join> joins = new ArrayList<>();
        Join e1 = new Join();
        ArrayList<ProjectionMapping> projectionMappings = new ArrayList<>();
        ProjectionMapping e2 = new ProjectionMapping();
        e2.setColumn("col");
        e2.setField("fie");
        projectionMappings.add(e2);
        e1.setProjectionMappings(projectionMappings);
        ArrayList<Table> tables = new ArrayList<>();
        Table e3 = new Table();
        e3.setName("table");
        tables.add(e3);
        e1.setTables(tables);
        joins.add(e1);
        sQLMapping.setJoins(joins);
        ArrayList<ColumnToField> columnToFieldMap = new ArrayList<>();
        ColumnToField e4 = new ColumnToField();
        columnToFieldMap.add(e4);
        sQLMapping.setColumnToFieldMap(columnToFieldMap);
        rdbms.setSQLMapping(sQLMapping);
        Bindings bindings = new Bindings();
        ArrayList<InOut> inList = new ArrayList<>();
        InOut e5 = new InOut();
        e5.setColumn("col");
        e5.setField(new Path("fie"));
        inList.add(e5);
        bindings.setInList(inList);
        delete.setBindings(bindings);

        //System.out.println(String.valueOf(rdbms));
    }

    public static Projection p = new Projection() {
        @Override
        public JsonNode toJson() {
            return null;
        }
    };

    @Test
    public void testInsert() throws Exception {
        cut.insert(c, p);
        assertNotNull(c.getErrors());
        assertTrue(c.getErrors().isEmpty());
    }


    @Test
    public void testDelete() throws Exception {
        FieldComparisonExpression fieldComparisonExpression = new FieldComparisonExpression(new Path("fie"), BinaryComparisonOperator._eq, new Path("fie"));
        cut.delete(c, fieldComparisonExpression);
        assertNotNull(c.getErrors());
        assertTrue(c.getErrors().isEmpty());
    }

    @Test
    public void testFind() throws Exception {
        FieldComparisonExpression fieldComparisonExpression = new FieldComparisonExpression(new Path("fie"), BinaryComparisonOperator._eq, new Path("fie"));
        cut.find(c, fieldComparisonExpression,new FieldProjection(new Path("fie"),true,false), null, null, null);
        assertNotNull(c.getErrors());
        assertTrue(c.getErrors().isEmpty());
    }

    @Test
    public void testSave() throws Exception {
        cut.save(c, true, null);
        assertNotNull(c.getErrors());
        assertTrue(c.getErrors().isEmpty());
    }

    @Test
    public void testUpdate() throws Exception {
        FieldComparisonExpression fieldComparisonExpression = new FieldComparisonExpression(new Path("fie"), BinaryComparisonOperator._eq, new Path("fie"));
        cut.update(c, fieldComparisonExpression, null, new FieldProjection(new Path("fie"),true,false));
        assertNotNull(c.getErrors());
        assertTrue(c.getErrors().isEmpty());
    }


}
