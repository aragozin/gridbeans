package org.gridkit.lab.gridbeans.monadic;

public interface MonadBuilder extends ExecutionTarget {

    public Checkpoint start();

    public Checkpoint checkpoint(String labelId);
    
    /**
     * Special locator to enforce bean execution of root location.
     */
    public ExecutionTarget root();
    

    public void rewind(Checkpoint label);

    public void sync();
    
    public void join(Checkpoint label);

    /**
     * Stashes current tracking context to stack. You can restore it later using {@link #pop()}.
     */
    public void push();
    
    /**
     * Discard current tracking context (<code>join({@link #finish()})<code>) and restores
     * context.
     */
    public void pop();

    /**
     * Adds tracked dependencies to context restored from stack.
     */
    public void exportAndPop();

    /**
     * Finalize and return execution graph. 
     */
    public Monad finish(); 
    
    // Utility methods
    
    /**
     * Rewind to beginning of time line.
     * <br/>
     * Equivalent of
     * <code>
     * <pre>
     * rewind(start());
     * </pre>
     * </code>
     */
    public void rewind();

    /**
     * Equivalent of
     * <code>
     * <pre>
     * push();
     * rewind();
     * joinable.join();
     * exportAndPop();
     * </pre>
     * </code>
     */
    public void join(Joinable joinable);
    
    /**
     * Equivalent of
     * <code>
     * <pre>
     * root().bean(Wallclock.class)
     * </pre>
     * </code>
     * 
     */
    public Wallclock wallclock();
}
