package com.redhat.lightblue.metadata.rdbms.converter;

class Flag {
    boolean asserted = false;

    public boolean isAsserted() {
        return asserted;
    }

    public void done(){
        asserted = true;
    }

    public void reset(){
        asserted = false;
    }
}
