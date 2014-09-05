package com.redhat.lightblue.metadata.rdbms.converter;

import com.redhat.lightblue.metadata.rdbms.util.Column;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by lcestari on 8/25/14.
 */
public class DynVar {
    private class Var {
        String keyName;
        Object value;
        Class<?> valueClass;
        Column column;

        public Var(Object o, Class<?> objectClass, Column column) {
            this.keyName = column.getAlias() == null? column.getName():column.getAlias();
            this.value = o;
            this.valueClass = objectClass;
            this.column = column;
        }
    }

    private Map<String,List<Var>> map;
    private RDBMSContext rdbmsContext;

    public DynVar(RDBMSContext rdbmsContext) {
        this(new HashMap<String, List<Var>>(),rdbmsContext);
    }

    DynVar(Map<String, List<Var>> map, RDBMSContext rdbmsContext) {
        this.map = map;
        this.rdbmsContext = rdbmsContext;
    }

    public void put(Object o, Class<?> objectClass, Column column) {
        Var var = new Var(o,objectClass, column);
        if(map.get(var.keyName) == null){
            map.put(var.keyName, new ArrayList<Var>());
        }
        map.get(var.keyName).add(var);
    }

    public void update(Object i, Column column) {
        String keyName = column.getAlias() == null? column.getName():column.getAlias();
        Var var = map.get(keyName).get(0);
        var.value = i;
    }

    public List getValues(String key){
        ArrayList list = new ArrayList();
        if( map.get(key) == null ||  map.get(key).isEmpty()){
            return list;
        }

        for (Var var : map.get(key)) {
            list.add(var.value);
        }
        return list;
    }

    public Set<String> getKeys(){
        return map.keySet();
    }

    public Class getFirstClassFromKey(String key){
        return map.get(key).get(0).valueClass;
    }
}

