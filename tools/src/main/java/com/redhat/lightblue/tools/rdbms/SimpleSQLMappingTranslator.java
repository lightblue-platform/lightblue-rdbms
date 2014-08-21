package com.redhat.lightblue.tools.rdbms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.redhat.lightblue.metadata.parser.Extensions;
import com.redhat.lightblue.metadata.parser.JSONMetadataParser;
import com.redhat.lightblue.metadata.rdbms.enums.LightblueOperators;
import com.redhat.lightblue.metadata.rdbms.enums.TypeOperators;
import com.redhat.lightblue.metadata.rdbms.impl.RDBMSPropertyParserImpl;
import com.redhat.lightblue.metadata.rdbms.model.*;
import com.redhat.lightblue.metadata.rdbms.model.Join;
import com.redhat.lightblue.metadata.types.DefaultTypes;
import org.hibernate.DuplicateMappingException;
import org.hibernate.cfg.JDBCBinderException;
import org.hibernate.cfg.reveng.DatabaseCollector;
import org.hibernate.cfg.reveng.TableIdentifier;
import org.hibernate.internal.util.StringHelper;
import org.hibernate.mapping.*;
import org.hibernate.mapping.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map;

/**
 * Created by lcestari on 8/19/14.
 */
public class SimpleSQLMappingTranslator implements Translator {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleSQLMappingTranslator.class);

    @Override
    public void translate(TranslatorContext tc) {
        DatabaseCollector collector = tc.getDatabaseCollector();
        Map<String, TableIdentifier> mapped = new HashMap<>();
        RDBMS rdbms = setupRDBMS(tc);

        rdbms.setDialect((String) tc.getMap().get("rdbmsDialect"));

        for (Iterator<Table> i = collector.iterateTables(); i.hasNext();) {
            Table table = i.next();
            if(table.getColumnSpan()==0) {
                LOGGER.warn("Table without column found and it will be ignored. Its name is '" + table + "'.");
                continue;
            }

            Map<String, TableIdentifier> fks = new HashMap<>();

            TableIdentifier tableIdentifier = TableIdentifier.create(table);
            String id = tableIdentifier.toString();

            if(!mapped.containsKey(id)) {
                mapped.put(id, tableIdentifier);
            } else {
                throw new IllegalStateException("Table mapped twice");
            }


            Join join = new Join();
            join.setProjectionMappings(new ArrayList<ProjectionMapping>());
            join.setTables(new ArrayList<com.redhat.lightblue.metadata.rdbms.model.Table>());

            for (Iterator<ForeignKey> j =  table.getForeignKeyIterator(); j.hasNext();) {
                ForeignKey fk = j.next();
                Table referencedTable = fk.getReferencedTable();
                TableIdentifier ti = TableIdentifier.create(referencedTable);
                for (Iterator<Column> z =  fk.getColumns().iterator(); z.hasNext();) {
                    Column c =  z.next();
                    fks.put(c.getName(),ti);
                }
            }


            Boolean mapfk = (Boolean) tc.getMap().get("mapfk");
            for (Iterator<Column> j = table.getColumnIterator(); j.hasNext();) {
                Column column = j.next();
                if(fks.get(column.getName())== null || mapfk ){
                    ColumnToField field = setupColumnToField(table, column);
                    rdbms.getSQLMapping().getColumnToFieldMap().add(field);

                    ProjectionMapping projectionMapping = setupProjectionMapping(column);
                    join.getProjectionMappings().add(projectionMapping);
                }
            }

            com.redhat.lightblue.metadata.rdbms.model.Table rdbmTable = new com.redhat.lightblue.metadata.rdbms.model.Table();
            rdbmTable.setName(table.getName());
            join.getTables().add(rdbmTable);

            rdbms.getSQLMapping().getJoins().add(join);
        }
    }

    protected ProjectionMapping setupProjectionMapping(Column column) {
        ProjectionMapping projectionMapping = new ProjectionMapping();
        projectionMapping.setColumn(column.getName());
        projectionMapping.setField(column.getName());
        projectionMapping.setSort(column.getName());
        return projectionMapping;
    }

    protected ColumnToField setupColumnToField(Table table, Column column) {
        ColumnToField field = new ColumnToField();
        field.setField(column.getName());
        field.setColumn(column.getName());
        field.setTable(table.getName());
        return field;
    }

    protected RDBMS setupRDBMS(TranslatorContext tc) {
        RDBMS rdbms = tc.getResult();

        if(rdbms.getSQLMapping() == null) {
            rdbms.setSQLMapping(new SQLMapping());
        }
        if(rdbms.getSQLMapping().getColumnToFieldMap() == null) {
            rdbms.getSQLMapping().setColumnToFieldMap(new ArrayList<ColumnToField>());
        }
        if(rdbms.getSQLMapping().getJoins() == null) {
            rdbms.getSQLMapping().setJoins(new ArrayList<Join>());
        }
        if(rdbms.getDelete() == null && rdbms.getFetch() == null && rdbms.getInsert() == null && rdbms.getSave() == null && rdbms.getUpdate() == null) {
            //at least one operation need to be informed
            Operation fetch = new Operation();
            fetch.setName(LightblueOperators.FETCH);
            fetch.setExpressionList(new ArrayList<Expression>());
            Statement statement = new Statement();
            statement.setSQL("select * from NEED_TO_CHANGE");
            statement.setType("select");
            fetch.getExpressionList().add(statement);
            rdbms.setFetch(fetch);
        }
        return rdbms;
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
