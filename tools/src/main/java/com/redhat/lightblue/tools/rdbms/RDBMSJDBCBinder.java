package com.redhat.lightblue.tools.rdbms;

import org.hibernate.cfg.JDBCBinder;
import org.hibernate.cfg.Mappings;
import org.hibernate.cfg.Settings;
import org.hibernate.cfg.reveng.DatabaseCollector;
import org.hibernate.cfg.reveng.ReverseEngineeringStrategy;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.Mapping;
import org.hibernate.service.spi.Stoppable;

import java.sql.SQLException;

/**
 * Created by lcestari on 8/19/14.
 */
public class RDBMSJDBCBinder extends JDBCBinder {
    private final ReverseEngineeringStrategy revEngStrategy;
    private final Mappings mappings;
    private final Settings settings;
    private final RDBMSConfiguration cfg;
    private ConnectionProvider connectionProvider;

    public RDBMSJDBCBinder(RDBMSConfiguration rdbmsConfiguration, Settings settings, Mappings mappings, ReverseEngineeringStrategy revEngStrategy) {
        super(rdbmsConfiguration,settings,mappings,revEngStrategy);
        this.cfg = rdbmsConfiguration;
        this.settings = settings;
        this.mappings = mappings;
        this.revEngStrategy = revEngStrategy;
    }

    @Override
    public void readFromDatabase(String catalog, String schema, Mapping mapping) {
        try {
            DatabaseCollector collector = readDatabaseSchema(catalog, schema);
            cfg.getTranslatorContext().setDatabaseCollector(collector);
        }
        catch (SQLException e) {
            JdbcServices jdbcServices = cfg.getServiceRegistry().getService(JdbcServices.class);
            throw jdbcServices.getSqlExceptionHelper().convert(e, "Reading from database", null);
        }
        finally	{
            JdbcServices jdbcServices = cfg.getServiceRegistry().getService(JdbcServices.class);
            this.connectionProvider = jdbcServices.getConnectionProvider();
            if ( connectionProvider instanceof Stoppable) {
                ( ( Stoppable ) connectionProvider ).stop();
            }
        }

    }
}
