package com.redhat.lightblue.tools.rdbms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.redhat.lightblue.metadata.parser.Extensions;
import com.redhat.lightblue.metadata.parser.JSONMetadataParser;
import com.redhat.lightblue.metadata.rdbms.enums.LightblueOperators;
import com.redhat.lightblue.metadata.rdbms.enums.TypeOperators;
import com.redhat.lightblue.metadata.rdbms.impl.RDBMSPropertyParserImpl;
import com.redhat.lightblue.metadata.rdbms.model.*;
import com.redhat.lightblue.metadata.types.DefaultTypes;
import org.hibernate.DuplicateMappingException;
import org.hibernate.cfg.JDBCBinderException;
import org.hibernate.cfg.reveng.DatabaseCollector;
import org.hibernate.cfg.reveng.TableIdentifier;
import org.hibernate.internal.util.StringHelper;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.RootClass;
import org.hibernate.mapping.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by lcestari on 8/19/14.
 */
public class SimpleSQLMappingTranslator implements Translator {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleSQLMappingTranslator.class);

    @Override
    public void translate(TranslatorContext tc) {
        DatabaseCollector collector = tc.getDatabaseCollector();
        RDBMS rdbms = tc.getResult();
        Map<String, TableIdentifier> mapped = new HashMap<>();
        rdbms.setSQLMapping(new SQLMapping());
        rdbms.getSQLMapping().setColumnToFieldMap(new ArrayList<ColumnToField>());
        rdbms.getSQLMapping().setJoins(new ArrayList<Join>());
        //at least one operation need to be informed
        Operation fetch = new Operation();
        fetch.setName(LightblueOperators.FETCH);
        fetch.setExpressionList(new ArrayList<Expression>());
        Statement statement = new Statement();
        statement.setSQL("select * from NEED_TO_CHANGE");
        statement.setType("select");
        fetch.getExpressionList().add(statement);
        rdbms.setFetch(fetch);

        for (Iterator<Table> i = collector.iterateTables(); i.hasNext();) {
            Table table = i.next();
            if(table.getColumnSpan()==0) {
                LOGGER.warn("Table without column found and it will be ignored. Its name is '" + table + "'.");
                continue;
            }

            TableIdentifier tableIdentifier = TableIdentifier.create(table);
            String id = tableIdentifier.toString();
            if(!mapped.containsKey(id)) {
                mapped.put(id, tableIdentifier);
            } else {
                throw new IllegalStateException("Table mapped twice");
            }
            rdbms.setDialect((String) tc.getMap().get("rdbmsDialect"));
            for (Iterator<Column> j = table.getColumnIterator(); j.hasNext();) {
                Column column = j.next();
                ColumnToField field = new ColumnToField();
                field.setField(column.getName());
                field.setColumn(column.getName());
                field.setTable(table.getName());
                rdbms.getSQLMapping().getColumnToFieldMap().add(field);

                Join join = new Join();
                join.setProjectionMappings(new ArrayList<ProjectionMapping>());
                ProjectionMapping projectionMapping = new ProjectionMapping();
                projectionMapping.setColumn(column.getName());
                projectionMapping.setField(column.getName());
                projectionMapping.setSort(column.getName());
                join.getProjectionMappings().add(projectionMapping);
                join.setTables(new ArrayList<com.redhat.lightblue.metadata.rdbms.model.Table>());
                com.redhat.lightblue.metadata.rdbms.model.Table rdbmTable = new com.redhat.lightblue.metadata.rdbms.model.Table();
                rdbmTable.setName(table.getName());
                join.getTables().add(rdbmTable);
                rdbms.getSQLMapping().getJoins().add(join);
            }
        }
    }

    @Override
    public void generateOutput(TranslatorContext tc) {
        JSONMetadataParser p;
        Extensions<JsonNode> x = new Extensions<>();
        x.addDefaultExtensions();
        RDBMSPropertyParserImpl parser = new RDBMSPropertyParserImpl();
        x.registerPropertyParser("rdbms", parser);
        p = new JSONMetadataParser(x, new DefaultTypes(), JsonNodeFactory.withExactBigDecimals(false));
        JsonNode parent = p.newNode();
        parser.convert(p, parent, tc.getResult());
        String r = parent.toString();
        LOGGER.debug("Result converted into JSON: "+ r);
        tc.getOutput().print(r);
    }
}
