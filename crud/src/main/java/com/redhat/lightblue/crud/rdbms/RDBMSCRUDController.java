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
package com.redhat.lightblue.crud.rdbms;

import com.redhat.lightblue.common.rdbms.RDBMSConstants;
import com.redhat.lightblue.metadata.rdbms.converter.RDBMSContext;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.redhat.lightblue.common.rdbms.RDBMSDataSourceResolver;
import com.redhat.lightblue.common.rdbms.RDBMSDataStore;
import com.redhat.lightblue.crud.*;
import com.redhat.lightblue.eval.FieldAccessRoleEvaluator;
import com.redhat.lightblue.eval.Projector;
import com.redhat.lightblue.metadata.EntityInfo;
import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.metadata.Metadata;
import com.redhat.lightblue.metadata.rdbms.model.RDBMS;
import com.redhat.lightblue.query.Projection;
import com.redhat.lightblue.query.QueryExpression;
import com.redhat.lightblue.query.Sort;
import com.redhat.lightblue.query.UpdateExpression;
import com.redhat.lightblue.util.Error;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 Implements the relationship between Metdata from Lightblue and the Data from RDBMS
 */
public class RDBMSCRUDController implements CRUDController {
    private static final Logger LOGGER = LoggerFactory.getLogger(RDBMSCRUDController.class);

    private final JsonNodeFactory nodeFactory;
    private final RDBMSDataSourceResolver rds;

    public RDBMSCRUDController(RDBMSDataSourceResolver rds) {
        this(JsonNodeFactory.withExactBigDecimals(true), rds);
    }

    public RDBMSCRUDController(JsonNodeFactory factory, RDBMSDataSourceResolver rds) {
        this.nodeFactory = factory;
        this.rds = rds;
    }

    @Override
    public CRUDInsertionResponse insert(CRUDOperationContext crudOperationContext, Projection projection) {
        LOGGER.debug("insert() start");
        Error.push("insert");
        //crudOperationContext.getDocuments(); // input? or maybe the projection mapping the values to be processed
        CRUDInsertionResponse response = new CRUDInsertionResponse();
        int n = 0;

        try {
            EntityMetadata md = crudOperationContext.getEntityMetadata(crudOperationContext.getEntityName());
            if (md.getAccess().getInsert().hasAccess(crudOperationContext.getCallerRoles())) {
                FieldAccessRoleEvaluator roleEval = new FieldAccessRoleEvaluator(md, crudOperationContext.getCallerRoles());
                RDBMSContext rdbmsContext = new RDBMSContext(null,null,null,projection,md,nodeFactory,rds,roleEval, crudOperationContext,"insert");

                RDBMSProcessor.process(rdbmsContext);

                crudOperationContext.getHookManager().queueHooks(crudOperationContext);

                n= rdbmsContext.getResultInteger() == null? 0 : rdbmsContext.getResultInteger() ;
            } else {
                crudOperationContext.addError(Error.get(RDBMSConstants.ERR_NO_ACCESS, "find:" + crudOperationContext.getEntityName()));
            }
        } finally {
            Error.pop();
        }

        response.setNumInserted(n);

        Error.pop();
        LOGGER.debug("insert() stop");
        return response;
    }

    @Override
    public CRUDSaveResponse save(CRUDOperationContext crudOperationContext, boolean upsert, Projection projection) {
        LOGGER.debug("save() start");
        Error.push("save");

        CRUDSaveResponse response = new CRUDSaveResponse();
        int n = 0;

        try {
            EntityMetadata md = crudOperationContext.getEntityMetadata(crudOperationContext.getEntityName());
            if (md.getAccess().getUpdate().hasAccess(crudOperationContext.getCallerRoles())  && md.getAccess().getInsert().hasAccess(crudOperationContext.getCallerRoles())) {
                FieldAccessRoleEvaluator roleEval = new FieldAccessRoleEvaluator(md, crudOperationContext.getCallerRoles());
                RDBMSContext rdbmsContext = new RDBMSContext(null,null,null,projection,md,nodeFactory,rds,roleEval, crudOperationContext,"save");

                RDBMSProcessor.process(rdbmsContext);

                crudOperationContext.getHookManager().queueHooks(crudOperationContext);

                n= rdbmsContext.getResultInteger();
            } else {
                crudOperationContext.addError(Error.get(RDBMSConstants.ERR_NO_ACCESS, "find:" + crudOperationContext.getEntityName()));
            }
        } finally {
            Error.pop();
        }

        response.setNumSaved(n);

        Error.pop();
        LOGGER.debug("save() stop");
        return response;
    }

    @Override
    public CRUDUpdateResponse update(CRUDOperationContext crudOperationContext,
                                     QueryExpression queryExpression,
                                     UpdateExpression updateExpression,
                                     Projection projection) {
        if (queryExpression == null) {
            throw new IllegalArgumentException("No queryExpression informed");
        }
        LOGGER.debug("update start: q:{} u:{} p:{}", queryExpression, updateExpression, projection);
        Error.push("update");

        CRUDUpdateResponse response = new CRUDUpdateResponse();

        try {
            EntityMetadata md = crudOperationContext.getEntityMetadata(crudOperationContext.getEntityName());
            if (md.getAccess().getUpdate().hasAccess(crudOperationContext.getCallerRoles())) {
                FieldAccessRoleEvaluator roleEval = new FieldAccessRoleEvaluator(md, crudOperationContext.getCallerRoles());
                RDBMSContext rdbmsContext = new RDBMSContext(null,null,queryExpression,projection,md,nodeFactory,rds,roleEval, crudOperationContext,"update");
                rdbmsContext.setUpdateExpression(updateExpression);
                RDBMSProcessor.process(rdbmsContext);

                crudOperationContext.getHookManager().queueHooks(crudOperationContext);
            } else {
                crudOperationContext.addError(Error.get(RDBMSConstants.ERR_NO_ACCESS, "find:" + crudOperationContext.getEntityName()));
            }
        } finally {
            Error.pop();
        }

        Error.pop();
        LOGGER.debug("update end: updated: {}, failed: {}", response.getNumUpdated(), response.getNumFailed());
        return response;
    }

    @Override
    public CRUDDeleteResponse delete(CRUDOperationContext crudOperationContext,
                                     QueryExpression queryExpression) {
        if (queryExpression == null) {
            throw new IllegalArgumentException("No queryExpression informed");
        }
        LOGGER.debug("delete start: q:{}", queryExpression);
        Error.push("delete");

        CRUDDeleteResponse response = new CRUDDeleteResponse();

        try {
            EntityMetadata md = crudOperationContext.getEntityMetadata(crudOperationContext.getEntityName());
            if (md.getAccess().getDelete().hasAccess(crudOperationContext.getCallerRoles())) {
                FieldAccessRoleEvaluator roleEval = new FieldAccessRoleEvaluator(md, crudOperationContext.getCallerRoles());
                RDBMSContext rdbmsContext = new RDBMSContext(null,null,queryExpression,null,md,nodeFactory,rds,roleEval, crudOperationContext,"delete");

                RDBMSProcessor.process(rdbmsContext);

                crudOperationContext.getHookManager().queueHooks(crudOperationContext);
            } else {
                crudOperationContext.addError(Error.get(RDBMSConstants.ERR_NO_ACCESS, "find:" + crudOperationContext.getEntityName()));
            }
        } finally {
            Error.pop();
        }

        Error.pop();
        LOGGER.debug("delete end: deleted: {}}", response.getNumDeleted());
        return response;
    }

    @Override
    public CRUDFindResponse find(CRUDOperationContext crudOperationContext,
                                 QueryExpression queryExpression,
                                 Projection projection,
                                 Sort sort,
                                 Long from,
                                 Long to) {
        if (queryExpression == null) {
            throw new IllegalArgumentException("No query informed");
        }
        if (projection == null) {
            throw new IllegalArgumentException("No projection informed");
        }
        LOGGER.debug("find start: q:{} p:{} sort:{} from:{} to:{}", queryExpression, projection, sort, from, to);
        Error.push("find");
        CRUDFindResponse response = new CRUDFindResponse();

        try {
            EntityMetadata md = crudOperationContext.getEntityMetadata(crudOperationContext.getEntityName());
            if (md.getAccess().getFind().hasAccess(crudOperationContext.getCallerRoles())) {
                FieldAccessRoleEvaluator roleEval = new FieldAccessRoleEvaluator(md, crudOperationContext.getCallerRoles());
                RDBMSContext rdbmsContext = new RDBMSContext(from,to,queryExpression,projection,md,nodeFactory,rds,roleEval, crudOperationContext,"find");

                RDBMSProcessor.process(rdbmsContext);

                crudOperationContext.getHookManager().queueHooks(crudOperationContext);
            } else {
                crudOperationContext.addError(Error.get(RDBMSConstants.ERR_NO_ACCESS, "find:" + crudOperationContext.getEntityName()));
            }
        } finally {
            Error.pop();
        }
        LOGGER.debug("find end: query: {} results: {}", response.getSize());
        return response;
    }

    @Override
    public void updateEntityInfo(Metadata md, EntityInfo ei) {

    }

    @Override
    public void newSchema(Metadata md, EntityMetadata emd) {

    }

    public JsonNodeFactory getNodeFactory() {
        return nodeFactory;
    }

    public RDBMSDataSourceResolver getRds() {
        return rds;
    }
}
