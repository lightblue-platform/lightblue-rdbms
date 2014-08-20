package com.redhat.lightblue.tools.rdbms;

import org.hibernate.cfg.reveng.DefaultReverseEngineeringStrategy;
import org.hibernate.cfg.reveng.ReverseEngineeringSettings;
import org.hibernate.cfg.reveng.ReverseEngineeringStrategy;

import java.io.FileNotFoundException;
import java.io.PrintStream;

/**
 * Created by lcestari on 8/19/14.
 */
public class RDBMSTools {
    private RDBMSConfiguration jmdc; // RDBMSConfiguration extends JDBCMetaDataConfiguration
    private TranslatorContext translatorContext;


    public void configure(){
        String fileName = "/tmp/rdbms.json";
        String hibernateDialect = "org.hibernate.dialect.H2Dialect";
        String rdbmsDialect = "oracle"; // it is the only implementation we have so far
        String jndi = "java:/mydatasource";
        /*
        TODO below list:
            Also get try to get the driver and the connection URL
            maven exec
            make maven exec default

         */

        boolean preferBasicCompositeIds = true;
        boolean detectManyToMany = true;
        boolean detectOneToOne = true;
        boolean detectOptimisticLock = true;

        try {
            translatorContext = new TranslatorContext.Builder(new SimpleSQLMappingTranslator(),new PrintStream(fileName)).build();
        } catch (FileNotFoundException e) {
            new IllegalStateException("Problem opening the file '"+fileName+"'",e);
        }
        translatorContext.getMap().put("rdbmsDialect",rdbmsDialect);
        jmdc = new RDBMSConfiguration(translatorContext);

        jmdc.setProperty("hibernate.dialect", hibernateDialect);
        jmdc.setProperty("hibernate.connection.datasource", jndi);

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

    public void translate(){
        jmdc.translate();
    }

    public void generateOutput(){
        jmdc.generateOutput();
    }

    public static void main(String[] args) {
        RDBMSTools rdbmsTools = new RDBMSTools();
        rdbmsTools.configure();
        rdbmsTools.read();
        rdbmsTools.translate();
        rdbmsTools.generateOutput();
    }
}
