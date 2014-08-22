package com.redhat.lightblue.tools.rdbms;

import com.redhat.lightblue.metadata.rdbms.model.RDBMS;
import org.hibernate.cfg.reveng.DatabaseCollector;

import java.io.File;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by lcestari on 8/19/14.
 */
public class TranslatorContext {
    protected Translator translator;
    protected java.io.PrintStream output;
    protected RDBMS result;
    protected RDBMS expectedResult;
    protected File file;
    // Reference to a map in case an  new extension wants to use other variable during the translation
    protected Map map;

    protected DatabaseCollector databaseCollector;

    protected TranslatorContext(Builder builder) {
        translator = builder.translator;
        output = builder.output;
        result = builder.result;
        expectedResult = builder.expectedResult;
        file = builder.file;
        map = builder.map;
    }

    public Translator getTranslator() {
        return translator;
    }

    public PrintStream getOutput() {
        return output;
    }

    public RDBMS getResult() {
        return result;
    }

    public RDBMS getExpectedResult() {
        return expectedResult;
    }

    public File getFile() {
        return file;
    }

    public Map getMap() {
        return map;
    }

    public DatabaseCollector getDatabaseCollector() {
        return databaseCollector;
    }

    public void setDatabaseCollector(DatabaseCollector databaseCollector) {
        this.databaseCollector = databaseCollector;
    }

    public static class Builder {
        protected Translator translator;
        protected java.io.PrintStream output;
        protected RDBMS result;
        protected RDBMS expectedResult;
        protected File file;
        protected Map map;

        public Builder(Translator translator, PrintStream output) {
            this.translator = translator;
            this.output = output;
        }

        public Builder preConfiguredResult(RDBMS result) {
            this.result = result;
            return this;
        }

        public Builder expectedResult(RDBMS expectedResult) {
            this.expectedResult = expectedResult;
            return this;
        }

        public Builder outputFile(File file) {
            this.file = file;
            return this;
        }

        public Builder otherConfiguration(Map map) {
            this.map = map;
            return this;
        }

        public TranslatorContext build() {
            if(result == null){
                result = new RDBMS();
            }
            if(map == null){
                map = new HashMap();
            }
            return new TranslatorContext(this);
        }
    }
}
