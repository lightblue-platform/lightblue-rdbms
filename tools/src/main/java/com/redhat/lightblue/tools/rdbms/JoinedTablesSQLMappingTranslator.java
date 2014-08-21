package com.redhat.lightblue.tools.rdbms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.redhat.lightblue.metadata.parser.Extensions;
import com.redhat.lightblue.metadata.parser.JSONMetadataParser;
import com.redhat.lightblue.metadata.rdbms.impl.RDBMSPropertyParserImpl;
import com.redhat.lightblue.metadata.rdbms.model.ColumnToField;
import com.redhat.lightblue.metadata.rdbms.model.Join;
import com.redhat.lightblue.metadata.rdbms.model.ProjectionMapping;
import com.redhat.lightblue.metadata.rdbms.model.RDBMS;
import com.redhat.lightblue.metadata.types.DefaultTypes;
import org.hibernate.cfg.reveng.DatabaseCollector;
import org.hibernate.cfg.reveng.TableIdentifier;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by lcestari on 8/19/14.
 */
public class JoinedTablesSQLMappingTranslator extends SimpleSQLMappingTranslator {
    private static final Logger LOGGER = LoggerFactory.getLogger(JoinedTablesSQLMappingTranslator.class);

    @Override
    public void translate(TranslatorContext tc) {
        DatabaseCollector collector = tc.getDatabaseCollector();
        Map<String, TableIdentifier> mapped = new HashMap<>();
        RDBMS rdbms = setupRDBMS(tc);
        Map<String, Join> tableToJoin = new HashMap<>();

        for (Iterator<Table> i = collector.iterateTables(); i.hasNext();) {
            Table table = i.next();
            if(table.getColumnSpan()==0) {
                LOGGER.warn("Table without column found and it will be ignored. Its name is '" + table + "'.");
                continue;
            }
            /*
            // TODO analyze this case
            if(revengStrategy.isManyToManyTable(table)) {

            }
            */
            TableIdentifier tableIdentifier = TableIdentifier.create(table);

            String id = tableIdentifier.toString();
            if(!mapped.containsKey(id)) {
                mapped.put(id, tableIdentifier);
            } else {
                throw new IllegalStateException("Table mapped twice");
            }

            Join join = null;
            if(tableToJoin.get(id) == null){
                join = new Join();
                join.setProjectionMappings(new ArrayList<ProjectionMapping>());
                join.setTables(new ArrayList<com.redhat.lightblue.metadata.rdbms.model.Table>());
                join.setJoinTablesStatement("");
                tableToJoin.put(id,join);
            }else{
                join = tableToJoin.get(id);
            }

            rdbms.setDialect((String) tc.getMap().get("rdbmsDialect"));
            for (Iterator<Column> j = table.getColumnIterator(); j.hasNext();) {
                Column column = j.next();
                ColumnToField field = setupColumnToField(table, column);
                rdbms.getSQLMapping().getColumnToFieldMap().add(field);



                ProjectionMapping projectionMapping = setupProjectionMapping(column);
                join.getProjectionMappings().add(projectionMapping);

                com.redhat.lightblue.metadata.rdbms.model.Table rdbmTable = new com.redhat.lightblue.metadata.rdbms.model.Table();
                rdbmTable.setName(table.getName());
                join.getTables().add(rdbmTable);

                String joinTablesStatement = join.getJoinTablesStatement();
                join.setJoinTablesStatement(joinTablesStatement + " AND " + genereteConditional());
                table.getForeignKeyIterator();

                rdbms.getSQLMapping().getJoins().add(join);
            }
        }
    }

    protected String genereteConditional() {
        return null;
    }
}
