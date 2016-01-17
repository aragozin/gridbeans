package org.gridkit.lab.gridbeans.monadic;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Set;

import org.gridkit.lab.gridbeans.ActionGraph;
import org.gridkit.lab.gridbeans.monadic.RuntimeTopology.TopologyNode;

public interface ExecutionGraph {

    public Start start();
    
    public Collection<Step> allSteps();

    public interface ScriptElement {

        /**
         * {@link Checkpoint} may have <code>null</code> host if it is global.
         */
        public TopologyNode host();
    }

    public interface Step extends ScriptElement {

        public Set<Step> dependencies();

        public Set<Step> dependents();
        
        public ActionGraph.ActionSite callSite();
    }

    public interface Start extends Step {

    }

    public interface PublishStep extends Step {
        
        public Bean bean();
        
        public Object[] lookupId();

        public ProducedBean outcome();
    }
    
    public interface CallStep extends Step {

        public Bean hostBean();

        public Method method();

        public Object[] params();

        public ProducedBean outcome();
    }

    public interface Checkpoint extends Step {

        public String label();

        public ActionGraph.ActionSite callSite();
    }

    public interface Bean extends ScriptElement {

        public Class<?> beanType();

    }

    public interface ProducedBean extends Bean {

        public Step producer();

    }

    public interface InjectedBean extends Bean {

        public Object[] lookupId();

        public ActionGraph.ActionSite callSite();
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
    
    public interface ExecutionObserver {

        public void onFire(CallDescription call);

        public void onComplete(CallDescription call);

        public void onCheckpoint(Checkpoint checkpoint);

        public void onFailure(Exception error);

        public void onFinish();
    }
}
