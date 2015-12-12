package org.gridkit.lab.gridbeans.monadic;

import java.util.concurrent.TimeUnit;

/**
 * This is special service provided by {@link RuntimeEnvironment}. 
 * <p>
 * {@link Wallclock} is used to implement delays in execution graph. 
 * 
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public interface Wallclock {

    public void delay(long time, TimeUnit tu);
    
}
