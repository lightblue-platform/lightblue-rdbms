/*
 Copyright 2013 Red Hat, Inc. and/or its affiliates.

 This file is part of lightblue.

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.redhat.lightblue.common.rdbms;

import javax.sql.DataSource;

/**
 * This interface helps to decouple the classes that needs its implementation to the implementation itself. This class is responsible to get a DataSource.
 * @author lcestari
 */
public interface RDBMSDataSourceResolver {

    /**
     * Find and get the DataSource that matches the requirements give by the RDBMSDataStore parameter
     * @param store
     *  The objects holds the parameters to find the right DataSource
     * @return
     *  The DataSource expected by the given RDBMSDataStore object or null or an Exception if something goes wrong
     */
    public DataSource get(RDBMSDataStore store);
}
