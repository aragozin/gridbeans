package org.gridkit.lab.gridbeans.monadic;

public interface DynamicTarget<T> {

    ExecutionTarget at(T locator);
    
}
