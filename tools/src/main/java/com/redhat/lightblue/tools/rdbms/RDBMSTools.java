package com.redhat.lightblue.tools.rdbms;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.JDBCMetaDataConfiguration;
import org.hibernate.cfg.reveng.DefaultReverseEngineeringStrategy;
import org.hibernate.cfg.reveng.OverrideRepository;
import org.hibernate.cfg.reveng.ReverseEngineeringSettings;
import org.hibernate.cfg.reveng.ReverseEngineeringStrategy;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.engine.jdbc.internal.JdbcServicesImpl;
import org.hibernate.engine.jdbc.spi.JdbcServices;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Properties;

/**
 * Created by lcestari on 8/19/14.
 */
public class RDBMSTools {
    private RDBMSConfiguration jmdc; // RDBMSConfiguration extends JDBCMetaDataConfiguration
    private TranslatorContext translatorContext;
    public void configure(){
        String fileName = "/tmp/rdbms.json";
        String dialect = "org.hibernate.dialect.H2Dialect";
        String jndi = "java:/mydatasource";

        boolean preferBasicCompositeIds = true;
        boolean detectManyToMany = true;
        boolean detectOneToOne = true;
        boolean detectOptimisticLock = true;

        try {
            translatorContext = new TranslatorContext.Builder(new SimpleSQLMappingTranslator(),new PrintStream(fileName)).build();
        } catch (FileNotFoundException e) {
            new IllegalStateException("Problem opening the file '"+fileName+"'",e);
        }
        jmdc = new RDBMSConfiguration(translatorContext);

        jmdc.setProperty("hibernate.dialect", dialect);
        jmdc.setProperty("hibernate.connection.datasource", jndi);
        //JdbcServicesImpl jdbcServices = (JdbcServicesImpl) jmdc.getServiceRegistry().getService(JdbcServices.class);
        //jdbcServices.getConnectionProvider();
        // -> jmdc.getServiceRegistry().getService( ConnectionProvider.class );
        //    ->jmdc.getServiceRegistry()
        //          ->
        //                serviceRegistry = new StandardServiceRegistryBuilder()
        //                        .applySettings(getProperties())
        //                        .build();

        jmdc.setPreferBasicCompositeIds(preferBasicCompositeIds);
        DefaultReverseEngineeringStrategy defaultStrategy = new DefaultReverseEngineeringStrategy();
        ReverseEngineeringStrategy strategy = defaultStrategy;
        ReverseEngineeringSettings qqsettings =
                new ReverseEngineeringSettings(strategy)
                        .setDefaultPackageName("com.redhat.lightblue.model")
                        .setDetectManyToMany( detectManyToMany )
                        .setDetectOneToOne( detectOneToOne )
                        .setDetectOptimisticLock( detectOptimisticLock );

        defaultStrategy.setSettings(qqsettings);
        strategy.setSettings(qqsettings);
        jmdc.setReverseEngineeringStrategy(strategy);


    }

    public void read(){
        jmdc.readFromJDBC();
    }

    public void tramslate(){
        jmdc.tramslate();
    }

    public static void main(String[] args) {

    }
}
