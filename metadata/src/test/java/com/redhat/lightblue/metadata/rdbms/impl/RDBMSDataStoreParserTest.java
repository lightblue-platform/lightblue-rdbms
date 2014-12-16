/*
 Copyright 2014 Red Hat, Inc. and/or its affiliates.

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.redhat.lightblue.common.rdbms.RDBMSConstants;
import com.redhat.lightblue.common.rdbms.RDBMSDataStore;
import com.redhat.lightblue.metadata.parser.Extensions;
import com.redhat.lightblue.metadata.parser.JSONMetadataParser;
import com.redhat.lightblue.metadata.parser.MetadataParser;
import com.redhat.lightblue.metadata.types.DefaultTypes;
import com.redhat.lightblue.util.JsonUtils;


public class RDBMSDataStoreParserTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testName(){
        assertEquals(RDBMSDataStoreParser.NAME, new RDBMSDataStoreParser<Object>().getDefaultName());
    }

    @Test
    public void testParse_InvalidName(){
        String invalidname = "FAKE";

        expectedEx.expect(com.redhat.lightblue.util.Error.class);
        expectedEx.expectMessage("{\"objectType\":\"error\",\"errorCode\":\""
                + RDBMSConstants.ERR_ILL_FORMED_METADATA
                + "\",\"msg\":\"" + invalidname + "\"}");

        new RDBMSDataStoreParser<Object>().parse(invalidname, null, null);
    }

    @Test
    public void testParse() throws IOException{
        String databaseName = "fakeDatabase";
        String datasourceName = "fakeDatasource";

        MetadataParser<JsonNode> p = new JSONMetadataParser(
                new Extensions<JsonNode>(),
                new DefaultTypes(),
                new JsonNodeFactory(true));

        JsonNode node = JsonUtils.json("{\"database\":\"" + databaseName + "\",\"datasource\":\"" + datasourceName + "\"}");
        RDBMSDataStore ds = new RDBMSDataStoreParser<JsonNode>().parse(RDBMSDataStoreParser.NAME, p, node);

        assertNotNull(ds);
        assertEquals(databaseName, ds.getDatabaseName());
        assertEquals(datasourceName, ds.getDatasourceName());
    }

    @Test
    public void testConvert() throws IOException{
        String databaseName = "fakeDatabase";
        String datasourceName = "fakeDatasource";

        MetadataParser<JsonNode> p = new JSONMetadataParser(
                new Extensions<JsonNode>(),
                new DefaultTypes(),
                new JsonNodeFactory(true));

        JsonNode emptyNode = JsonUtils.json("{}");

        RDBMSDataStore ds = new RDBMSDataStore(databaseName, datasourceName);

        new RDBMSDataStoreParser<JsonNode>().convert(p, emptyNode, ds);

        assertEquals(databaseName, p.getStringProperty(emptyNode, "database"));
        assertEquals(datasourceName, p.getStringProperty(emptyNode, "datasource"));
    }

    @Test
    public void testConvert_WithNullValues() throws IOException{
        MetadataParser<JsonNode> p = new JSONMetadataParser(
                new Extensions<JsonNode>(),
                new DefaultTypes(),
                new JsonNodeFactory(true));

        JsonNode emptyNode = JsonUtils.json("{}");

        RDBMSDataStore ds = new RDBMSDataStore(null, null);

        new RDBMSDataStoreParser<JsonNode>().convert(p, emptyNode, ds);

        assertNull(p.getStringProperty(emptyNode, "database"));
        assertNull(p.getStringProperty(emptyNode, "datasource"));
    }

}
