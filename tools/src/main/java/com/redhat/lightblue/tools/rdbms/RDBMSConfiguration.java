package com.redhat.lightblue.tools.rdbms;

import org.hibernate.MappingException;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.JDBCBinder;
import org.hibernate.cfg.JDBCMetaDataConfiguration;
import org.hibernate.cfg.reveng.DefaultReverseEngineeringStrategy;
import org.hibernate.engine.spi.Mapping;
import org.hibernate.id.factory.IdentifierGeneratorFactory;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Property;
import org.hibernate.type.Type;

/**
 * Created by lcestari on 8/19/14.
 */
public class RDBMSConfiguration extends JDBCMetaDataConfiguration {

    private TranslatorContext tc;

    public RDBMSConfiguration(TranslatorContext tc) {
        this.tc = tc;
    }

    @Override
    public void readFromJDBC() {
        JDBCBinder binder = new RDBMSJDBCBinder(this, buildSettings(), createMappings(), new DefaultReverseEngineeringStrategy());
        binder.readFromDatabase(null, null, buildMapping(this));
    }

    // private class from org.hibernate.cfg.JDBCMetaDataConfiguration
    protected static Mapping buildMapping(final Configuration cfg) {
        return new Mapping() {
            /**
             * Returns the identifier type of a mapped class
             */
            public Type getIdentifierType(String persistentClass) throws MappingException {
                PersistentClass pc = cfg.getClassMapping( persistentClass );
                if (pc==null) throw new MappingException("persistent class not known: " + persistentClass);
                return pc.getIdentifier().getType();
            }

            public String getIdentifierPropertyName(String persistentClass) throws MappingException {
                final PersistentClass pc = cfg.getClassMapping( persistentClass );
                if (pc==null) throw new MappingException("persistent class not known: " + persistentClass);
                if ( !pc.hasIdentifierProperty() ) return null;
                return pc.getIdentifierProperty().getName();
            }

            public Type getReferencedPropertyType(String persistentClass, String propertyName) throws MappingException
            {
                final PersistentClass pc = cfg.getClassMapping( persistentClass );
                if (pc==null) throw new MappingException("persistent class not known: " + persistentClass);
                Property prop = pc.getProperty(propertyName);
                if (prop==null)  throw new MappingException("property not known: " + persistentClass + '.' + propertyName);
                return prop.getType();
            }

            public IdentifierGeneratorFactory getIdentifierGeneratorFactory() {
                return null;
            }
        };
    }

    public void translate() {
        tc.getTranslator().translate(tc);
    }

    public void generateOutput() {
        tc.getTranslator().generateOutput(tc);
    }

    public TranslatorContext getTranslatorContext() {
        return tc;
    }
}
