package org.gridkit.lab.gridbeans.monadic;

public class Period {

    private final Label begin;
    private final Label end;

    public Period(Label begin, Label end) {
        this.begin = begin;
        this.end = end;
    }
    
    public Label begin() {
        return begin;
    }

    public Label end() {
        return end;
    }
}
