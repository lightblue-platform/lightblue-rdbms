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
package com.redhat.lightblue.metadata.rdbms.converter;

/**
 * Created by lcestari on 9/10/14.
 */
public class Range {

    private final Long from;
    private final Long to;

    public Range() {
        this(null,null);
    }

    public Range(Long from, Long to) {
        this.from = from;
        this.to = to;
    }

    public Long getLimit() {
        if(to != null && from != null) {
            return to - from; // after the offset (M rows skipped), the remaining will be limited
        }else{
            return to;
        }
    }

    public Long getOffset() {
        return from;
    }

    public boolean isConfigured() {
        if(to != null || from != null) {
            return true;
        }
        return false;
    }
}
