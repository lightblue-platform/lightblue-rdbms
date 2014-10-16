package com.redhat.lightblue.metadata.rdbms.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.redhat.lightblue.util.JsonUtils;

import java.sql.Types;

/**
* Created by lcestari on 8/26/14.
*/
public class Column {
    private int position;
    private String name;
    private String alias;
    private String clazz;
    private int type;
    private boolean temp;

    public Column(int position, String name, String alias, String clazz, int type) {
        this.setPosition(position);
        this.setName(name);
        this.setAlias(alias);
        this.setClazz(clazz);
        this.setType(type);
    }

    public static Column createTemp(String name, String canonicalName){
        Column column = new Column(0, name, name, canonicalName, Types.JAVA_OBJECT);
        column.temp = true;
        return column;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getClazz() {
        return clazz;
    }

    public void setClazz(String clazz) {
        this.clazz = clazz;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public boolean isTemp() {
        return temp;
    }

    public void setTemp(boolean temp) {
        this.temp = temp;
    }

    @Override
    public String toString() {
        JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(true);

        ObjectNode objectNode = jsonNodeFactory.objectNode();

        JsonNode jsonNode = jsonNodeFactory.numberNode(position);
        objectNode.set("position", jsonNode);

        jsonNode = name == null? jsonNodeFactory.nullNode() : jsonNodeFactory.textNode(name);
        objectNode.set("name", jsonNode);

        jsonNode = alias == null? jsonNodeFactory.nullNode() : jsonNodeFactory.textNode(alias);
        objectNode.set("alias", jsonNode);

        jsonNode = clazz == null? jsonNodeFactory.nullNode() : jsonNodeFactory.textNode(clazz);
        objectNode.set("clazz", jsonNode);

        jsonNode = jsonNodeFactory.numberNode(type);
        objectNode.set("type", jsonNode);

        jsonNode = jsonNodeFactory.booleanNode(temp);
        objectNode.set("temp", jsonNode);

        return JsonUtils.prettyPrint(objectNode);
    }
}
