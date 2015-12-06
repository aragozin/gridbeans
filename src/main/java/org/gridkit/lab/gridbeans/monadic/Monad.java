package org.gridkit.lab.gridbeans.monadic;

import org.gridkit.lab.gridbeans.ActionGraph;
import org.gridkit.lab.gridbeans.monadic.spi.MonadExecutionEnvironment;

/**
 * Encapsulated definition of complex execution graph.
 * 
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public interface Monad {

    public ExecutionClosure bind(MonadExecutionEnvironment environment);

    public interface ExecutionClosure {
        
        public void execute(ExecutionObserver observer);
        
    }
    
    public interface ExecutionObserver {
        
        public void fire(CallDescription call);

        public void complete(CallDescription call);
        
        public void checkpoint(String name);

        public void finish();
    }
    
    public interface CallDescription {
        
        public int getCallId();
        
        public Object getExecutionHost();

        public Object getBeanReference();

        public ActionGraph.ActionSite getCallSite();

        public String[] getParamDescription();

        public boolean isVoid();
        
        public String getResultDescription();

        public Throwable getException();
        
    }
}
