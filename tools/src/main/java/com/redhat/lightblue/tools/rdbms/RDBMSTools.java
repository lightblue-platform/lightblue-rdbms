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

    // parameters and default values
    String fileName = "/tmp/rdbms.json";
    String hibernateDialect = "org.hibernate.dialect.H2Dialect";
    String rdbmsDialect = "oracle"; // it is the only implementation we have so far
    String url = null;
    String username = null;
    String password = null;
    String jndi = "java:/mydatasource";
    Boolean mapfk = Boolean.parseBoolean("false");
    String canonicalName = SimpleSQLMappingTranslator.class.getCanonicalName();
    String driverClass = null; // The driver must be in jar defined in pom.xml using system path, maybe change to the following approach:
    /*
        ClassLoader loader = URLClassLoader.newInstance(new URL[]{new File("pathToJar").toURL()}, getClass().getClassLoader());
        Class<?> clazz = Class.forName(driverClass, true, loader);
        Constructor<?> c = clazz.getConstructor();
        c.newInstance(); // but hibernate would need just the driver class name, which wont see this classloader

        maybe using:
        ClassLoader currentThreadClassLoader = Thread.currentThread().getContextClassLoader();
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{new File("pathToJar").toURL()}, currentThreadClassLoader);
        Thread.currentThread().setContextClassLoader(myClassLoader);
        as the hibernate seems to use ReflectHelper to get the class (which will use the thread class loader)
     */


    public void configure(){


        Class<? extends Translator> clazz = null; // or JoinedTablesSQLMappingTranslator
        try {
            clazz = (Class<? extends Translator>) Class.forName(canonicalName);
        } catch (ClassNotFoundException e) {
            new IllegalStateException("Problem creating the informed Translator class '"+canonicalName+"'",e);
        }
        /*
        TODO below list:
            Also get try to get the driver and the connection URL + driver.jar
            Merge
            Generate the rest of lightblue json schema
            tests
         */

        boolean preferBasicCompositeIds = true;
        boolean detectManyToMany = true;
        boolean detectOneToOne = true;
        boolean detectOptimisticLock = true;

        try {
            translatorContext = new TranslatorContext.Builder(clazz.newInstance(),new PrintStream(fileName)).build();
        } catch (FileNotFoundException e) {
            new IllegalStateException("Problem opening the file '"+fileName+"'",e);
        } catch (InstantiationException e) {
            new IllegalStateException("Problem creating the informed Translator class '"+canonicalName+"'",e);
        } catch (IllegalAccessException e) {
            new IllegalStateException("Problem creating the informed Translator class '"+canonicalName+"'",e);
        }

        translatorContext.getMap().put("rdbmsDialect",rdbmsDialect);
        translatorContext.getMap().put("mapfk",mapfk);
        translatorContext.getMap().put("TranslatorClass",clazz);

        jmdc = new RDBMSConfiguration(translatorContext);

        jmdc.setProperty("hibernate.dialect", hibernateDialect);

        if(url != null && driverClass != null){
            jmdc.setProperty("hibernate.connection.driver_class", driverClass);
            jmdc.setProperty("hibernate.connection.url", url);
            if(username != null) {
                jmdc.setProperty("hibernate.connection.username", username);
                jmdc.setProperty("hibernate.connection.password", password);
            }
        }
        else if(jndi != null && !jndi.isEmpty()) {
            jmdc.setProperty("hibernate.connection.datasource", jndi);
        } else {
            new IllegalStateException("No connection to the database was informed");
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
            if(s[0].equals("fileName")){
                rdbmsTools.fileName = s[1];
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
            }else{
                System.err.println("Invalid argument: "+args[i]);
            }
        }
        return rdbmsTools;
    }
}
