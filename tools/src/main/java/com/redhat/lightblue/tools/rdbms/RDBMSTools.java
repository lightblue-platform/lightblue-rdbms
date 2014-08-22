package com.redhat.lightblue.tools.rdbms;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.hibernate.cfg.reveng.DefaultReverseEngineeringStrategy;
import org.hibernate.cfg.reveng.ReverseEngineeringSettings;
import org.hibernate.cfg.reveng.ReverseEngineeringStrategy;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Created by lcestari on 8/19/14.
 */
public class RDBMSTools {

    static
    {
        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.INFO);
        rootLogger.addAppender(new ConsoleAppender(
                new PatternLayout("%-6r [%p] %c - %m%n")));
    }

    private RDBMSConfiguration jmdc; // RDBMSConfiguration extends JDBCMetaDataConfiguration
    private TranslatorContext translatorContext;

    // parameters and default values
    String outputFile;
    String hibernateDialect;
    String rdbmsDialect = "oracle";
    String url = null;
    String username = null;
    String password = null;
    String jndi;
    Boolean mapfk = Boolean.parseBoolean("false");
    String canonicalName = SimpleSQLMappingTranslator.class.getCanonicalName();
    String driverClass = null;
    String pathToJar;


    public void configure() {
        Class<? extends Translator> clazz = null; // or JoinedTablesSQLMappingTranslator
        try {
            clazz = (Class<? extends Translator>) Class.forName(canonicalName);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Problem creating the informed Translator class '" + canonicalName + "'", e);
        }
        /*
        TODO below list:
            Also get try to get the driver and the connection URL + driver.jar
            Merge - read the file
            Generate the rest of lightblue json schema
            tests
            tests h2 parameter
         */

        boolean preferBasicCompositeIds = true;
        boolean detectManyToMany = true;
        boolean detectOneToOne = true;
        boolean detectOptimisticLock = true;

        try {
            if (outputFile != null) {
                translatorContext = new TranslatorContext.Builder(clazz.newInstance(), new PrintStream(outputFile)).build();
            } else {
                translatorContext = new TranslatorContext.Builder(clazz.newInstance(), System.out).build();
            }
        } catch (FileNotFoundException e) {
            throw new IllegalStateException("Problem opening the file '" + outputFile + "'", e);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IllegalStateException("Problem creating the informed Translator class '" + canonicalName + "'", e);
        }

        translatorContext.getMap().put("rdbmsDialect", rdbmsDialect);
        translatorContext.getMap().put("mapfk", mapfk);
        translatorContext.getMap().put("TranslatorClass", clazz);

        jmdc = new RDBMSConfiguration(translatorContext);

        if (hibernateDialect != null) {
            jmdc.setProperty("hibernate.dialect", hibernateDialect);
        } else {
            throw new IllegalStateException("hibernateDialect not informed");
        }

        if (url != null && driverClass != null && pathToJar != null) {
            jmdc.setProperty("hibernate.connection.driver_class", driverClass);
            jmdc.setProperty("hibernate.connection.url", url);
            if (username != null) {
                jmdc.setProperty("hibernate.connection.username", username);
                jmdc.setProperty("hibernate.connection.password", password);
            }
        } else if (jndi != null && !jndi.isEmpty()) {
            jmdc.setProperty("hibernate.connection.datasource", jndi);
        } else {
            throw new IllegalStateException("No connection to the database was informed (pathToJar or url or driver_class or jndi)");
        }

        if (pathToJar != null){
            ClassLoader currentThreadClassLoader = Thread.currentThread().getContextClassLoader();
            try {
                URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{new File(pathToJar).toURL()}, currentThreadClassLoader);
                Thread.currentThread().setContextClassLoader(urlClassLoader);
            } catch (MalformedURLException e) {
                throw new IllegalStateException(e);
            }
        }


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

        RDBMSTools rdbmsTools =  processArgs(args);
        rdbmsTools.configure();
        rdbmsTools.read();
        rdbmsTools.translate();
        rdbmsTools.generateOutput();
    }

    private static RDBMSTools processArgs(String[] args) {
        RDBMSTools rdbmsTools = new RDBMSTools();
        for (int i = 0; i < args.length; i++) {
            String[] s = args[i].split("=");
            if(s[0].equals("outputFile")){
                rdbmsTools.outputFile = s[1];
            } else if (s[0].equals("hibernateDialect")){
                rdbmsTools.hibernateDialect = s[1];
            }else if (s[0].equals("rdbmsDialect")){
                rdbmsTools.rdbmsDialect = s[1];
            }else if (s[0].equals("url")){
                rdbmsTools.url = s[1];
            }else if (s[0].equals("driverClass")){
                rdbmsTools.driverClass = s[1];
            }else if (s[0].equals("username")){
                rdbmsTools.username = s[1];
            }else if (s[0].equals("password")){
                rdbmsTools.password = s[1];
            }else if (s[0].equals("jndi")){
                rdbmsTools.jndi = s[1];
            }else if (s[0].equals("mapfk")){
                rdbmsTools.mapfk = Boolean.parseBoolean(s[1]);
            }else if (s[0].equals("canonicalName")){
                rdbmsTools.canonicalName = s[1];
            }else if (s[0].equals("pathToJar")){
                rdbmsTools.pathToJar = s[1];
            }else{
                System.err.println("Invalid argument: "+args[i]);
            }
        }
        return rdbmsTools;
    }
}
