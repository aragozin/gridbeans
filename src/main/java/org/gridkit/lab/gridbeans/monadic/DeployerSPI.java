package org.gridkit.lab.gridbeans.monadic;

/**
 * This is service provided by execution environment
 * and used to place beans from prototype.
 *  
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public interface DeployerSPI {

    public <T, B extends T> T deploy(Class<T> type, B prototype);
    
}
