package org.gridkit.lab.gridbeans.monadic;

import org.gridkit.lab.gridbeans.ActionGraph;

/**
 * Encapsulated definition of complex execution graph.
 * 
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public interface ExecutionGraph {

    public ExecutionClosure bind(RuntimeEnvironment environment);

    public interface ExecutionClosure {
        
        public void execute(ExecutionObserver observer);
        
    }
    
    public interface ExecutionObserver {
        
        public void onFire(CallDescription call);

        public void onComplete(CallDescription call);
        
        public void onCheckpoint(CheckpointDescription checkpoint);

        public void onFailure(Exception error);
        
        public void onFinish();
    }
    
    public interface CheckpointDescription {
        
        public String getName();
        
    }
    
    public interface CallDescription {
        
        public int getCallId();
        
        public Object getExecutionHost();

        public Object getBeanReference();

        public ActionGraph.ActionSite getCallSite();

        public String[] getParamDescription();

        public boolean hasOutput();
        
        public String getResultDescription();

        public Throwable getException();
        
    }
}
