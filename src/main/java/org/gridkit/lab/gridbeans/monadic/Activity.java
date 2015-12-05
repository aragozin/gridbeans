package org.gridkit.lab.gridbeans.monadic;

public interface Activity extends Joinable {

    public void stop();
    
    public void join();
    
}
