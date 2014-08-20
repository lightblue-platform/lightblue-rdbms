package com.redhat.lightblue.tools.rdbms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.redhat.lightblue.metadata.parser.Extensions;
import com.redhat.lightblue.metadata.parser.JSONMetadataParser;
import com.redhat.lightblue.metadata.rdbms.impl.RDBMSPropertyParserImpl;
import com.redhat.lightblue.metadata.rdbms.model.RDBMS;
import com.redhat.lightblue.metadata.types.DefaultTypes;
import org.hibernate.cfg.reveng.DatabaseCollector;
import org.hibernate.cfg.reveng.TableIdentifier;
import org.hibernate.mapping.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        RDBMS rdbms = tc.getResult();
        Map<String, TableIdentifier> mapped = new HashMap<>();

        Map oneToManyCandidates = collector.getOneToManyCandidates();

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


            /*
            PrimaryKeyInfo pki = bindPrimaryKeyToProperties(table, rc, processed, mapping, collector);
            bindColumnsToVersioning(table, rc, processed, mapping);
            bindOutgoingForeignKeys(table, rc, processed);
            bindColumnsToProperties(table, rc, processed, mapping);
            List incomingForeignKeys = (List) manyToOneCandidates.get( rc.getEntityName() );
            bindIncomingForeignKeys(rc, processed, incomingForeignKeys, mapping);
            updatePrimaryKey(rc, pki);
            */

        }
    }
}
