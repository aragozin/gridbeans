package org.gridkit.lab.gridbeans.monadic;

public interface MonadBuilder extends ExecutionTarget {

    public Label start();

    public Label finish();
    
    public Label label(String labelId);
    
    /**
     * Special locator to enforce bean execution of root location.
     */
    public ExecutionTarget root();
    
    public void rewind(Label label);

    public void join(Label label);

    public void push();
    
    public void pop();

    // Utility methods
    
    public void join(Joinable joinable);
    
    public Wallclock wallclock();
}
