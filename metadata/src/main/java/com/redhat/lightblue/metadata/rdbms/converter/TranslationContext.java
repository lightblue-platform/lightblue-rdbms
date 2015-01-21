package com.redhat.lightblue.metadata.rdbms.converter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.common.rdbms.RDBMSConstants;
import com.redhat.lightblue.crud.CRUDOperationContext;
import com.redhat.lightblue.metadata.FieldTreeNode;
import com.redhat.lightblue.metadata.Type;
import com.redhat.lightblue.metadata.rdbms.enums.LightblueOperators;
import com.redhat.lightblue.metadata.rdbms.model.ColumnToField;
import com.redhat.lightblue.metadata.rdbms.model.InOut;
import com.redhat.lightblue.metadata.rdbms.model.Join;
import com.redhat.lightblue.metadata.rdbms.model.ProjectionMapping;
import com.redhat.lightblue.query.ArrayQueryMatchProjection;
import com.redhat.lightblue.query.ArrayRangeProjection;
import com.redhat.lightblue.query.FieldProjection;
import com.redhat.lightblue.query.Projection;
import com.redhat.lightblue.query.ProjectionList;
import com.redhat.lightblue.query.Value;
import com.redhat.lightblue.util.Error;
import com.redhat.lightblue.util.Path;

/**
* Created by lcestari on 9/22/14.
*/
public class TranslationContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(TranslationContext.class);
  
    private Translator translator;
    CRUDOperationContext crudOperationContext;
    RDBMSContext rdbmsContext;
    FieldTreeNode fieldTreeNode;
    Map<String, ProjectionMapping> fieldToProjectionMap;
    Map<ProjectionMapping, Join> projectionToJoinMap;
    Map<String, ColumnToField> fieldToTablePkMap;
    SelectStmt sortDependencies;
    Set<String> nameOfTables;
    boolean needDistinct;
    boolean notOp;

    // temporary variables
    Path tmpArray;
    Type tmpType;
    List<Value> tmpValues;

    public boolean hasJoins;
    public boolean hasSortOrLimit;

    LinkedList<SelectStmt> firstStmts; // Useful for complex queries which need to run before the  main one
    SelectStmt baseStmt;
    List<Map.Entry<String,List<String>>> logicalStmt;

    public TranslationContext(Translator translator, RDBMSContext rdbmsContext, FieldTreeNode fieldTreeNode) {
        this.translator = translator;
        this.firstStmts = new LinkedList<>();
        this.fieldToProjectionMap = new HashMap<>();
        this.fieldToTablePkMap = new HashMap<>();
        this.sortDependencies = new SelectStmt(translator);
        this.sortDependencies.setOrderBy(new ArrayList<String>());
        this.projectionToJoinMap = new HashMap<>();
        this.nameOfTables = new HashSet<>();
        this.baseStmt =  new SelectStmt(translator);
        this.logicalStmt =  new ArrayList<>();
        this.crudOperationContext = rdbmsContext.getCrudOperationContext();
        this.rdbmsContext = rdbmsContext;
        this.fieldTreeNode = fieldTreeNode;
        index();
    }

    public List<SelectStmt> generateFinalTranslation(){
        ArrayList<SelectStmt> result = new ArrayList<>();
        SelectStmt lastStmt = new SelectStmt(translator);

        for (SelectStmt stmt : firstStmts) {
            fillDefault(stmt);
            result.add(stmt);
        }

        Projection p = rdbmsContext.getProjection();
        List<String> resultColumns = new ArrayList<>();
        processProjection(p,resultColumns);
        if(resultColumns.size() == 0){
            if(rdbmsContext.getCRUDOperationName() != LightblueOperators.DELETE) {
                throw com.redhat.lightblue.util.Error.get(RDBMSConstants.ERR_NO_PROJECTION, p != null ? p.toString() : "Projection is null");
            } else{
                rdbmsContext.getIn();
                for (Object o : rdbmsContext.getIn()) {
                    InOut io = (InOut)o;
                    String column = fieldToProjectionMap.get(io.getField().toString()).getColumn();
                    resultColumns.add(column);
                }
                if(resultColumns.size() == 0){
                    throw Error.get(RDBMSConstants.ERR_ILL_FORMED_METADATA, "Delete operation need In variables to process");
                }
            }
        }
        lastStmt.setResultColumns(resultColumns);
        fillDefault(lastStmt);
        result.add(lastStmt);

        return result;
    }

    private void fillDefault(SelectStmt selectStmt) {
        selectStmt.setFromTables(baseStmt.getFromTables());
        selectStmt.setWhereConditionals(baseStmt.getWhereConditionals());
        selectStmt.setOrderBy(sortDependencies.getOrderBy());
        selectStmt.setRange(rdbmsContext.getFromToQueryRange());
    }

    private void processProjection(Projection projection, List<String> resultColumns) {
        LOGGER.debug("processProjection(projection: {}, resultColumns: {})", projection.toString(), resultColumns.toString());
        if(projection instanceof ProjectionList){
            ProjectionList projectionList = (ProjectionList) projection;
            for (Projection projection1 : projectionList.getItems()) {
                processProjection(projection1,resultColumns);
            }
        }else if (projection instanceof ArrayRangeProjection) {
            ArrayRangeProjection i = (ArrayRangeProjection) projection;
            throw Error.get(RDBMSConstants.ERR_SUP_OPERATOR, projection.toString());
        }else if (projection instanceof ArrayQueryMatchProjection) {
            ArrayQueryMatchProjection i = (ArrayQueryMatchProjection) projection;
            throw Error.get(RDBMSConstants.ERR_SUP_OPERATOR, projection.toString());
        }else if (projection instanceof FieldProjection) {
            FieldProjection fieldProjection = (FieldProjection) projection;
            String sField = Translator.translatePath(fieldProjection.getField());
            String column = fieldToProjectionMap.get(sField).getColumn();

            InOut in = new InOut();
            InOut out = new InOut();
            in.setColumn(column);
            out.setColumn(column);
            in.setField(fieldProjection.getField());
            out.setField(fieldProjection.getField());

            this.rdbmsContext.getIn().add(in);
            this.rdbmsContext.getOut().add(out);
            resultColumns.add(column);
        }
    }

    private void index() {
        for (Join join : rdbmsContext.getRdbms().getSQLMapping().getJoins()) {
            for (ProjectionMapping projectionMapping : join.getProjectionMappings()) {
                String field = projectionMapping.getField();
                fieldToProjectionMap.put(field, projectionMapping);
                projectionToJoinMap.put(projectionMapping, join);
            }
            needDistinct = join.isNeedDistinct() || needDistinct;
        }
        for (ColumnToField columnToField : rdbmsContext.getRdbms().getSQLMapping().getColumnToFieldMap()) {
            fieldToTablePkMap.put(columnToField.getField(), columnToField);
        }
    }

    public void clearTmp() {
        this.tmpArray = null;
        this.tmpType = null;
        this.tmpValues = null;
    }

    public void clearAll(){
        firstStmts.clear();
        fieldToProjectionMap.clear();
        this.firstStmts = null;
        this.crudOperationContext = null;
        this.rdbmsContext = null;
        this.fieldTreeNode = null;
        this.clearTmp();
    }

    public void checkJoins() {
        if (nameOfTables.size() > 1) {
            hasJoins = true;
        }
    }
}
