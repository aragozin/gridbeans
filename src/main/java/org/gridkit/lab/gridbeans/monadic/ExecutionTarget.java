package org.gridkit.lab.gridbeans.monadic;

public interface ExecutionTarget {

    public <T extends Locator> T locator(Class<T> type);
    
    public <T> T bean(Class<T> type, Object... lookupKeys);

    @BeanShortcut(beanType = DeployerSPI.class)
    public <T, B extends T> T deploy(Class<T> intf, B bean);
}
