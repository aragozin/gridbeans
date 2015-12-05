package org.gridkit.lab.gridbeans.monadic;

public interface ExecutionTarget {

    public <P, T extends DynamicTarget<P>> T locator(Class<T> type);
    
    public <T> T bean(Class<T> type);

    public <T> T bean(Class<T> type, Object identity);
    
    public <T> T deploy(T bean);
}
