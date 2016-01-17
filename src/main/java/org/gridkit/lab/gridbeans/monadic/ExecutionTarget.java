package org.gridkit.lab.gridbeans.monadic;

public interface ExecutionTarget {

    public <T extends Locator> T locator(Class<T> type);
    
    public <T> T bean(Class<T> type, Object... lookupKeys);

    /**
     * Publish allow you to bind produced bean to well-known name,
     * so it will be available to be resolved via {@link #bean(Class, Object...)} call.
     * <p>
     * Bean identity cannot be published more than once for single execution target.
     * <br>
     * Some additional ambiguity resolution restriction applies.
     */
    // Special method
    public void publish(Object bean, Object... lookupKeys);

    @BeanShortcut(beanType = DeployerSPI.class)
    public <T, B extends T> T deploy(Class<T> intf, B bean);
}
