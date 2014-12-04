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

import static com.redhat.lightblue.util.test.AbstractJsonNodeTest.loadJsonNode;
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
import com.redhat.lightblue.metadata.parser.Extensions;
import com.redhat.lightblue.metadata.parser.JSONMetadataParser;
import com.redhat.lightblue.metadata.parser.MetadataParser;
import com.redhat.lightblue.metadata.rdbms.enums.DialectOperators;
import com.redhat.lightblue.metadata.rdbms.model.RDBMS;
import com.redhat.lightblue.metadata.types.DefaultTypes;
import com.redhat.lightblue.util.JsonUtils;

public class RDBMSPropertyParserImplTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test(expected = IllegalArgumentException.class)
    public void testConvert_NullObject() throws IOException{
        MetadataParser<JsonNode> p = new JSONMetadataParser(
                new Extensions<JsonNode>(),
                new DefaultTypes(),
                new JsonNodeFactory(true));

        JsonNode emptyNode = JsonUtils.json("{}");

        new RDBMSPropertyParserImpl<JsonNode>().convert(p, emptyNode, null);
    }

    @Test
    public void testConvert() throws IOException{
        MetadataParser<JsonNode> p = new JSONMetadataParser(
                new Extensions<JsonNode>(),
                new DefaultTypes(),
                new JsonNodeFactory(true));

        JsonNode node = loadJsonNode("RdbmsMetadataTest-fetch.json");

        RDBMS rdbms = new RDBMSPropertyParserImpl<JsonNode>().parse(RDBMSPropertyParserImpl.NAME, p, node.get("rdbms"));

        MetadataParser<JsonNode> p2 = new JSONMetadataParser(
                new Extensions<JsonNode>(),
                new DefaultTypes(),
                new JsonNodeFactory(true));

        new RDBMSPropertyParserImpl<JsonNode>().convert(p2, node, rdbms);

        assertNotNull(p2.getObjectProperty(node, "rdbms"));
    }

    @Test
    public void testParse_InvalidName(){
        String invalidname = "FAKE";

        expectedEx.expect(com.redhat.lightblue.util.Error.class);
        expectedEx.expectMessage("{\"objectType\":\"error\",\"errorCode\":\""
                + RDBMSConstants.ERR_WRONG_ROOT_NODE_NAME
                + "\",\"msg\":\"Node name informed:" + invalidname + "\"}");

        new RDBMSPropertyParserImpl<Object>().parse(invalidname, null, null);
    }

    @Test
    public void testParse_NoDialect() throws IOException{
        expectedEx.expect(com.redhat.lightblue.util.Error.class);
        expectedEx.expectMessage("{\"objectType\":\"error\",\"errorCode\":\""
                + RDBMSConstants.ERR_FIELD_REQUIRED
                + "\",\"msg\":\"No field informed\"}");

        MetadataParser<JsonNode> p = new JSONMetadataParser(
                new Extensions<JsonNode>(),
                new DefaultTypes(),
                new JsonNodeFactory(true));

        JsonNode emptyNode = JsonUtils.json("{}");

        new RDBMSPropertyParserImpl<JsonNode>().parse(RDBMSPropertyParserImpl.NAME, p, emptyNode);
    }

    @Test
    public void testParse_EmptyDialect() throws IOException{
        expectedEx.expect(com.redhat.lightblue.util.Error.class);
        expectedEx.expectMessage("{\"objectType\":\"error\",\"errorCode\":\""
                + RDBMSConstants.ERR_FIELD_REQUIRED
                + "\",\"msg\":\"No field informed\"}");

        MetadataParser<JsonNode> p = new JSONMetadataParser(
                new Extensions<JsonNode>(),
                new DefaultTypes(),
                new JsonNodeFactory(true));

        JsonNode emptyNode = JsonUtils.json("{\"dialect\":\"\"}");

        new RDBMSPropertyParserImpl<JsonNode>().parse(RDBMSPropertyParserImpl.NAME, p, emptyNode);
    }

    @Test
    public void testParse_NoOperation() throws IOException{
        expectedEx.expect(com.redhat.lightblue.util.Error.class);
        expectedEx.expectMessage("{\"objectType\":\"error\",\"errorCode\":\""
                + RDBMSConstants.ERR_FIELD_REQUIRED
                + "\",\"msg\":\"No Operation informed\"}");

        MetadataParser<JsonNode> p = new JSONMetadataParser(
                new Extensions<JsonNode>(),
                new DefaultTypes(),
                new JsonNodeFactory(true));

        JsonNode emptyNode = JsonUtils.json("{\"dialect\":\""+ DialectOperators.ORACLE + "\"}");

        new RDBMSPropertyParserImpl<JsonNode>().parse(RDBMSPropertyParserImpl.NAME, p, emptyNode);
    }

    @Test
    public void testParse_Delete() throws IOException{
        MetadataParser<JsonNode> p = new JSONMetadataParser(
                new Extensions<JsonNode>(),
                new DefaultTypes(),
                new JsonNodeFactory(true));

        JsonNode node = loadJsonNode("RdbmsMetadataTest-delete.json");

        RDBMS rdbms = new RDBMSPropertyParserImpl<JsonNode>().parse(RDBMSPropertyParserImpl.NAME, p, node.get("rdbms"));

        assertEquals(DialectOperators.ORACLE, rdbms.getDialect());
        assertNotNull(rdbms.getDelete());
        assertNull(rdbms.getFetch());
        assertNull(rdbms.getInsert());
        assertNull(rdbms.getSave());
        assertNull(rdbms.getUpdate());
    }

    @Test
    public void testParse_Fetch() throws IOException{
        MetadataParser<JsonNode> p = new JSONMetadataParser(
                new Extensions<JsonNode>(),
                new DefaultTypes(),
                new JsonNodeFactory(true));

        JsonNode node = loadJsonNode("RdbmsMetadataTest-fetch.json");

        RDBMS rdbms = new RDBMSPropertyParserImpl<JsonNode>().parse(RDBMSPropertyParserImpl.NAME, p, node.get("rdbms"));

        assertEquals(DialectOperators.ORACLE, rdbms.getDialect());
        assertNull(rdbms.getDelete());
        assertNotNull(rdbms.getFetch());
        assertNull(rdbms.getInsert());
        assertNull(rdbms.getSave());
        assertNull(rdbms.getUpdate());
    }

    @Test
    public void testParse_Insert() throws IOException{
        MetadataParser<JsonNode> p = new JSONMetadataParser(
                new Extensions<JsonNode>(),
                new DefaultTypes(),
                new JsonNodeFactory(true));

        JsonNode node = loadJsonNode("RdbmsMetadataTest-insert.json");

        RDBMS rdbms = new RDBMSPropertyParserImpl<JsonNode>().parse(RDBMSPropertyParserImpl.NAME, p, node.get("rdbms"));

        assertEquals(DialectOperators.ORACLE, rdbms.getDialect());
        assertNull(rdbms.getDelete());
        assertNull(rdbms.getFetch());
        assertNotNull(rdbms.getInsert());
        assertNull(rdbms.getSave());
        assertNull(rdbms.getUpdate());
    }

    @Test
    public void testParse_Save() throws IOException{
        MetadataParser<JsonNode> p = new JSONMetadataParser(
                new Extensions<JsonNode>(),
                new DefaultTypes(),
                new JsonNodeFactory(true));

        JsonNode node = loadJsonNode("RdbmsMetadataTest-save.json");

        RDBMS rdbms = new RDBMSPropertyParserImpl<JsonNode>().parse(RDBMSPropertyParserImpl.NAME, p, node.get("rdbms"));

        assertEquals(DialectOperators.ORACLE, rdbms.getDialect());
        assertNull(rdbms.getDelete());
        assertNull(rdbms.getFetch());
        assertNull(rdbms.getInsert());
        assertNotNull(rdbms.getSave());
        assertNull(rdbms.getUpdate());
    }

    @Test
    public void testParse_Update() throws IOException{
        MetadataParser<JsonNode> p = new JSONMetadataParser(
                new Extensions<JsonNode>(),
                new DefaultTypes(),
                new JsonNodeFactory(true));

        JsonNode node = loadJsonNode("RdbmsMetadataTest-update.json");

        RDBMS rdbms = new RDBMSPropertyParserImpl<JsonNode>().parse(RDBMSPropertyParserImpl.NAME, p, node.get("rdbms"));

        assertEquals(DialectOperators.ORACLE, rdbms.getDialect());
        assertNull(rdbms.getDelete());
        assertNull(rdbms.getFetch());
        assertNull(rdbms.getInsert());
        assertNull(rdbms.getSave());
        assertNotNull(rdbms.getUpdate());
    }

}
