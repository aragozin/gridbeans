package org.gridkit.lab.gridbeans.monadic;

public class Period {

    private final Checkpoint begin;
    private final Checkpoint end;

    public Period(Checkpoint begin, Checkpoint end) {
        this.begin = begin;
        this.end = end;
    }
    
    public Checkpoint begin() {
        return begin;
    }

    public Checkpoint end() {
        return end;
    }
}
