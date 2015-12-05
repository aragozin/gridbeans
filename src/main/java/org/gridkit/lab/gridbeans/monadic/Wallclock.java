package org.gridkit.lab.gridbeans.monadic;

import java.util.concurrent.TimeUnit;

public interface Wallclock {

    public void delay(long time, TimeUnit tu);
    
}
