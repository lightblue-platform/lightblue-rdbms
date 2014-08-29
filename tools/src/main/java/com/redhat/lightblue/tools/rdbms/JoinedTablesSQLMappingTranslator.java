package com.redhat.lightblue.tools.rdbms;

import com.redhat.lightblue.metadata.rdbms.model.ColumnToField;
import com.redhat.lightblue.metadata.rdbms.model.Join;
import com.redhat.lightblue.metadata.rdbms.model.ProjectionMapping;
import com.redhat.lightblue.metadata.rdbms.model.RDBMS;
import org.hibernate.cfg.reveng.DatabaseCollector;
import org.hibernate.cfg.reveng.TableIdentifier;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.ForeignKey;
import org.hibernate.mapping.PrimaryKey;
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
        Map<TableIdentifier,PrimaryKey> pks = new HashMap<>();

        rdbms.setDialect((String) tc.getMap().get("rdbmsDialect"));

        for (Iterator<Table> i = collector.iterateTables(); i.hasNext();) {
            Table table = i.next();
            if(table.getColumnSpan()==0) {
                LOGGER.warn("Table without column found and it will be ignored. Its name is '" + table + "'.");
                continue;
            }
            /*
            // TODO analyze this case
            if(revengStrategy.isManyToManyTable(table)) {}
            */

            Map<String, ForeignKey> fks = new HashMap<>();

            TableIdentifier tableIdentifier = TableIdentifier.create(table);
            String id = tableIdentifier.toString();

            pks.put(tableIdentifier,table.getPrimaryKey());

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
                rdbms.getSQLMapping().getJoins().add(join);
                tableToJoin.put(id,join);
            }else{
                join = tableToJoin.get(id);
            }

            for (Iterator<ForeignKey> j =  table.getForeignKeyIterator(); j.hasNext();) {
                ForeignKey fk = j.next();
                Table referencedTable = fk.getReferencedTable();
                TableIdentifier ti = TableIdentifier.create(referencedTable);
                tableToJoin.put(ti.toString(),join);
                for (Iterator<Column> z =  fk.getColumns().iterator(); z.hasNext();) {
                    Column c =  z.next();
                    fks.put(c.getName(),fk);
                    String joinTable = join.getJoinTablesStatement();
                    if(joinTable.length() !=  0){
                        joinTable = joinTable + " AND ";
                    }
                    join.setJoinTablesStatement(joinTable + table.getName() + "." + c.getName() + "=" + referencedTable.getName() + "." + c.getName());
                }
            }

            for (Iterator<Column> j = table.getColumnIterator(); j.hasNext();) {
                Column column = j.next();
                if(fks.get(column.getName())== null ){
                    ColumnToField field = setupColumnToField(table, column);
                    rdbms.getSQLMapping().getColumnToFieldMap().add(field);

                    ProjectionMapping projectionMapping = setupProjectionMapping(column);
                    join.getProjectionMappings().add(projectionMapping);
                }
            }

            com.redhat.lightblue.metadata.rdbms.model.Table rdbmTable = new com.redhat.lightblue.metadata.rdbms.model.Table();
            rdbmTable.setName(table.getName());
            join.getTables().add(rdbmTable);
        }
    }

    protected String genereteConditional() {
        return null;
    }
}
