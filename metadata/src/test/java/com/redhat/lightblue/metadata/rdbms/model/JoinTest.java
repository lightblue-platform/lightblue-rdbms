package com.redhat.lightblue.metadata.rdbms.model;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.redhat.lightblue.metadata.parser.Extensions;
import com.redhat.lightblue.metadata.parser.JSONMetadataParser;
import com.redhat.lightblue.metadata.parser.MetadataParser;
import com.redhat.lightblue.metadata.types.DefaultTypes;
import com.redhat.lightblue.util.JsonUtils;

public class JoinTest {

    @Test
    public void testParse_DistinctTrue() throws IOException{
        MetadataParser<JsonNode> p = new JSONMetadataParser(
                new Extensions<JsonNode>(),
                new DefaultTypes(),
                new JsonNodeFactory(true));

        JsonNode node = JsonUtils.json("{\"tables\":[{\"name\":\"tname\"}],\"needDistinct\":true,\"projectionMappings\":[{\"column\":\"c\",\"field\":\"f\"}]}");

        Join join = new Join();
        join.parse(p, node);

        assertTrue(join.isNeedDistinct());
    }

    @Test
    public void testParse_DistinctFalse() throws IOException{
        MetadataParser<JsonNode> p = new JSONMetadataParser(
                new Extensions<JsonNode>(),
                new DefaultTypes(),
                new JsonNodeFactory(true));

        JsonNode node = JsonUtils.json("{\"tables\":[{\"name\":\"tname\"}],\"needDistinct\":false,\"projectionMappings\":[{\"column\":\"c\",\"field\":\"f\"}]}");

        Join join = new Join();
        join.parse(p, node);

        assertFalse(join.isNeedDistinct());
    }

}
