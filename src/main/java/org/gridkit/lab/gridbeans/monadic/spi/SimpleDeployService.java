package org.gridkit.lab.gridbeans.monadic.spi;

import org.gridkit.lab.gridbeans.monadic.DeployerSPI;

/**
 * This is simple implementation of {@link DeployerSPI} service. 
 * 
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 *
 */
public class SimpleDeployService implements DeployerSPI {

    @Override
    public <T, B extends T> T deploy(Class<T> type, B prototype) {
        return prototype;
    }
}
