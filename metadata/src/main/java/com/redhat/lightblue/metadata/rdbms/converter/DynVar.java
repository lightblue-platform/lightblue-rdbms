package com.redhat.lightblue.metadata.rdbms.converter;

import com.redhat.lightblue.metadata.rdbms.util.Column;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        map.get(var.keyName).add(var);
    }

    public void update(Object i, Column column) {
        String keyName = column.getAlias() == null? column.getName():column.getAlias();
        Var var = map.get(keyName).get(0);
        var.value = i;
    }


    public List getValues(String key){
        ArrayList list = new ArrayList();
        for (Var var : map.get(key)) {
            list.add(var.value);
        }
        return list;
    }

    public static void main(String[] args) {
        //Testing the array of bytes
        byte[] x = new byte[]{(byte)0, (byte)1};
        System.out.println(x);
        System.out.println(x.getClass());
        System.out.println(x.hashCode());
        Class<?> c = x.getClass();
        if (c.isArray()) {
            System.out.format("            Type: %s%n"
                            + "  Component Type: %s%n",
                   c, c.getComponentType());
        }
    }
}
