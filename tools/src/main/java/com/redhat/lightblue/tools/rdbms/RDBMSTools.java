package com.redhat.lightblue.tools.rdbms;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.hibernate.cfg.reveng.DefaultReverseEngineeringStrategy;
import org.hibernate.cfg.reveng.ReverseEngineeringSettings;
import org.hibernate.cfg.reveng.ReverseEngineeringStrategy;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.*;

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
    String rdbmsDialect = "oracle";
    Boolean mapfk = false;
    Boolean test = false;
    String strategyCanonicalName = SimpleSQLMappingTranslator.class.getCanonicalName();
    String outputFile;
    String hibernateDialect;
    String url;
    String username;
    String password;
    String jndi;
    String driverClass;
    String pathToJar;


    public void configure() {
        Class<? extends Translator> clazz = null; // or JoinedTablesSQLMappingTranslator
        try {
            clazz = (Class<? extends Translator>) Class.forName(strategyCanonicalName);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Problem creating the informed Translator class '" + strategyCanonicalName + "'", e);
        }
        /*
        TODO below list:
            Merge - read the file
            Generate the rest of lightblue json schema
            tests
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
            throw new IllegalStateException("Problem creating the informed Translator class '" + strategyCanonicalName + "'", e);
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
        if(args.length == 0 || (args.length == 1 && "help".equals(args[0].replaceAll("-",""))) ){
            System.out.println("Usage:\n" +
                    "\tkey=value\n" +
                    " Where the keys are:\n" +
                    "\trdbmsDialect: dialect that will be used in RDBMS module;\n" +
                    "\tmapfk: if the rev. eng. will map the FK using the Simple strategy mapping;\n" +
                    "\ttest: create some tables on an h2 database to test;\n" +
                    "\tstrategyCanonicalName: The canonical name of the class to be used for mapping;\n" +
                    "\toutputFile: path to the output content be persisted;\n" +
                    "\thibernateDialect: the dialect used by the hibernate;\n" +
                    "\turl: the URL to connect to the database ;\n" +
                    "\tusername: the user name of the connection  ;\n" +
                    "\tpassword: the password of the connection  ;\n" +
                    "\tjndi: The JNDI address to be looked up  ;\n" +
                    "\tdriverClass: The canonical name of the driver for the database;\n" +
                    "\tpathToJar: the path to the jar with the driver");
        }

        RDBMSTools rdbmsTools =  processArgs(args);
        rdbmsTools.configure();
        if(rdbmsTools.test){
            generateLoad(rdbmsTools);
        }
        rdbmsTools.read();
        rdbmsTools.translate();
        rdbmsTools.generateOutput();
    }

    private static void generateLoad(RDBMSTools rdbmsTools) {
        try {
            //Class<?> aClass = Thread.currentThread().getContextClassLoader().loadClass(rdbmsTools.driverClass);
            //Class.forName(rdbmsTools.driverClass);
            Class<?> aClass = Thread.currentThread().getContextClassLoader().loadClass("org.h2.jdbcx.JdbcDataSource");
            DataSource ds = (DataSource) aClass.newInstance();
            Method msetURL = aClass.getMethod("setURL", String.class);
            Method msetUser = aClass.getMethod("setUser", String.class);
            Method msetPassword = aClass.getMethod("setPassword", String.class);
            msetURL.invoke(ds, rdbmsTools.url);
            msetUser.invoke(ds, rdbmsTools.username);
            msetPassword.invoke(ds, rdbmsTools.password);
            //Connection conn = DriverManager.getConnection(rdbmsTools.url, rdbmsTools.username, rdbmsTools.password);
            Connection conn = ds.getConnection();
            createTables(conn);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException | SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void createTables(Connection conn) {
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE People ( PersonID INT PRIMARY KEY, Name VARCHAR(255) );");
            stmt.execute("CREATE TABLE Document ( DocID INT PRIMARY KEY, PersonID INT, FOREIGN KEY(PersonID) REFERENCES People(PersonID) );");
            // Good resource for examples of procedure with h2 https://code.google.com/p/h2database/source/browse/trunk/h2/src/test/org/h2/samples/Function.java
            stmt.execute("CREATE ALIAS getVersion FOR \"org.h2.engine.Constants.getVersion\"");
            ResultSet rs = stmt.executeQuery("CALL getVersion()");
            if (rs.next()) {
                System.out.println("Version: " + rs.getString(1));
            }
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
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
            }else if (s[0].equals("strategyCanonicalName")){
                rdbmsTools.strategyCanonicalName = s[1];
            }else if (s[0].equals("pathToJar")){
                rdbmsTools.pathToJar = s[1];
            }else if (s[0].equals("test")){
                rdbmsTools.test =  Boolean.parseBoolean(s[1]);
            }else{
                System.err.println("Invalid argument: "+args[i]);
            }
        }
        return rdbmsTools;
    }
}
