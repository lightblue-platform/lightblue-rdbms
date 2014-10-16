package com.redhat.lightblue.common.rdbms;

import org.junit.Test;

import static org.junit.Assert.*;

public class RDBMSDataStoreTest {

    @Test
    public void testPojo(){
        RDBMSDataStore cut = new RDBMSDataStore();
        cut.setDatabaseName("DBTest");
        assertEquals("DBTest", cut.getDatabaseName());
        cut.setDatasourceName("DSTest");
        assertEquals("DSTest",cut.getDatasourceName());
        assertEquals(RDBMSDataStore.BACKEND,cut.getBackend());
        RDBMSDataStore expected = new RDBMSDataStore();
        expected.setDatabaseName("DBTest");
        expected.setDatasourceName("DSTest");
        assertEquals(expected,expected);
        assertEquals(expected.hashCode(),expected.hashCode());
        assertEquals(expected.toString(),expected.toString());
    }

}