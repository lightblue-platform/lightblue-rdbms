package com.redhat.lightblue.config.rdbms;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.redhat.lightblue.config.ControllerConfiguration;
import com.redhat.lightblue.config.DataSourcesConfiguration;
import com.redhat.lightblue.crud.CRUDController;
import com.redhat.lightblue.crud.rdbms.RDBMSCRUDController;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class RDBMSControllerFactoryTest {

    RDBMSControllerFactory cut;

    @Before
    public void before(){
        cut = new RDBMSControllerFactory();
    }
    @After
    public void after(){
        cut = null;
    }

    @Test
    public void testCreateController() throws Exception {
        DataSourcesConfiguration ds = new DataSourcesConfiguration();
        ControllerConfiguration cfg = new ControllerConfiguration();
        RDBMSCRUDController controller = (RDBMSCRUDController) cut.createController(cfg, ds);
        assertEquals(JsonNodeFactory.withExactBigDecimals(true),controller.getNodeFactory());
        assertEquals( new RDBMSDataSourceMap(ds),controller.getRds());
    }
}