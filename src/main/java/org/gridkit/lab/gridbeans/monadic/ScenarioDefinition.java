package org.gridkit.lab.gridbeans.monadic;

import org.gridkit.lab.gridbeans.monadic.ExecutionGraph.ExecutionObserver;

/**
 * Encapsulated definition of complex execution graph.
 * Script is captured in abstract form and is not bound to topology.
 * 
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public interface ScenarioDefinition {

    public ExecutionClosure bind(RuntimeEnvironment environment);

    public interface ExecutionClosure {
        
        public void execute(ExecutionObserver observer);
        
    }
}
