package org.gridkit.lab.gridbeans.monadic.builder;

import java.util.List;

import org.gridkit.lab.gridbeans.ActionGraph;
import org.gridkit.lab.gridbeans.ActionGraph.Action;

class RawGraphData {

    CheckpointInfo[] checkpoints;
    ActionGraph.Bean omniLocator;
    ActionGraph.Bean rootLocator;
    ActionGraph.Bean[] locations;
    
    
    static class CheckpointInfo {

        int id;
        String name;
        boolean scoped;
        
        ActionGraph.Action[] dependencies;
        ActionGraph.Action[] dependents;
        
        StackTraceElement[] site;

        public CheckpointInfo(int id, String name, boolean scoped, List<Action> dependencies, List<Action> dependents, StackTraceElement[] site) {
            this.id = id;
            this.name = name;
            this.dependencies = dependencies.toArray(new ActionGraph.Action[0]);
            this.dependents = dependents.toArray(new ActionGraph.Action[0]);
            this.site = site;
            this.scoped = scoped;
        }

        public String toString() {
            return id == 0 ? "<start>" : 
                   name == null ? "<" + id + ">" : name;
        }
        
    }
}
