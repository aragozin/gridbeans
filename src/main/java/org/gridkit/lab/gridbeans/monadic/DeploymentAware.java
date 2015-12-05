package org.gridkit.lab.gridbeans.monadic;

/**
 * This interface may be used to implement smart beans,
 * which form a certain way of communications after being
 * replicated and deployed to multiple deployment targets.
 *  
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public interface DeploymentAware {

    /**
     * Optionally bean can clone itself for each deployment.
     */
    public Object newInstance();
    
    public void localDeploy(TargetId deploymentTarget);
    
    public void globalDeploy();
    
}
