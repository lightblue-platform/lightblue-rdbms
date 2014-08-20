package com.redhat.lightblue.tools.rdbms;

/**
 * Created by lcestari on 8/19/14.
 */
public interface Translator {
    void translate(TranslatorContext tc);
    void generateOutput(TranslatorContext tc);
}
