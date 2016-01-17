package org.gridkit.lab.gridbeans.monadic.spi;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.gridkit.lab.gridbeans.ActionGraph.ActionSite;
import org.gridkit.lab.gridbeans.monadic.ExecutionGraph;
import org.gridkit.lab.gridbeans.monadic.RuntimeTopology.TopologyNode;

public class SimpleExecutionGraph implements ExecutionGraph {

    private List<StepImpl> steps = new ArrayList<StepImpl>();
    
    @Override
    public Start start() {
        StartImpl si = new StartImpl();
        for(StepImpl s: steps) {
            if (s.dependencies.isEmpty()) {
                si.dependents.add(s);
            }
        }
        return si;
    }

    @Override
    public Collection<Step> allSteps() {
        return Collections.<Step>unmodifiableCollection(steps);
    }
    
    public InjectedBean inject(TopologyNode host, Class<?> type, Object[] params, ActionSite callSite) {
        InjectedBeanImpl b = new InjectedBeanImpl();
        b.host = host;
        b.type = type;
        b.params = params;
        b.callSite = callSite;
        
        return b;
    }

    public ProducedBean produce(Class<?> type, CallStep step) {
        CallStepImpl csi = (CallStepImpl) step;
        BeanImpl bean = new BeanImpl();
        bean.producer = csi;
        bean.type = type;
        csi.outcome = bean;
        return bean;
    }

    public ProducedBean produce(PublishStep step) {
        PublishStepImpl psi = (PublishStepImpl) step;
        BeanImpl bean = new BeanImpl();
        bean.producer = psi;
        bean.type = psi.bean.beanType();
        psi.outcome = bean;
        return bean;
    }
    
    public CallStep call(Bean host, Method m, Object[] params, ActionSite callSite) {
        CallStepImpl csi = new CallStepImpl();
        csi.bean = host;
        csi.host = csi.bean.host();
        csi.method = m;
        csi.params = params;
        csi.callSite = callSite;
        
        if (host instanceof ProducedBean) {
            depends(csi, ((ProducedBean) host).producer());
        }
        for(Object p: params) {
            if (p instanceof ProducedBean) {
                depends(csi, ((ProducedBean) p).producer());
            }
        }
        
        steps.add(csi);
        return csi;
    }

    public PublishStep publish(Bean bean, Object[] identity, ActionSite callSite) {
        PublishStepImpl csi = new PublishStepImpl();
        csi.bean = bean;
        csi.host = csi.bean.host();
        csi.lookupId = identity;
        csi.callSite = callSite;
        
        if (bean instanceof ProducedBean) {
            depends(csi, ((ProducedBean) bean).producer());
        }
        
        steps.add(csi);
        return csi;
    }
    
    public Checkpoint checkpoint(String name, TopologyNode host, ActionSite site) {
        CheckpointImpl ci = new CheckpointImpl();
        ci.label = name;
        ci.host = host;
        ci.callSite = site;
        steps.add(ci);
        return ci;
    }
    
    public void depends(Step dependent, Step dependency) {
        ((StepImpl)dependent).dependencies.add(dependency);
        ((StepImpl)dependency).dependents.add(dependent);
    }

    private abstract class StepImpl implements Step {
        
        TopologyNode host;
        Set<Step> dependencies = new LinkedHashSet<ExecutionGraph.Step>();
        Set<Step> dependents = new LinkedHashSet<ExecutionGraph.Step>();
        
        @Override
        public TopologyNode host() {
            return host;
        }
        
        @Override
        public Set<Step> dependencies() {
            return dependencies;
        }
        
        @Override
        public Set<Step> dependents() {
            return dependents;
        }
    }

    private class StartImpl extends StepImpl implements Start {

        @Override
        public ActionSite callSite() {
            return null;
        }
    }
    
    private class CheckpointImpl extends StepImpl implements Checkpoint {

        String label;
        ActionSite callSite;
        
        @Override
        public String label() {
            return label;
        }

        @Override
        public ActionSite callSite() {
            return callSite;
        }
    }
    
    private interface Action extends Step {
        
    }
    
    private class PublishStepImpl extends StepImpl implements PublishStep, Action {

        Bean bean;
        Object[] lookupId;
        ActionSite callSite;
        ProducedBean outcome;
        
        @Override
        public ActionSite callSite() {
            return callSite;
        }
        
        @Override
        public Bean bean() {
            return bean;
        }
        
        @Override
        public Object[] lookupId() {
            return lookupId;
        }

        @Override
        public ProducedBean outcome() {
            return outcome;
        }
    }
    
    private class CallStepImpl extends StepImpl implements CallStep, Action {

        Bean bean;
        Method method;
        ActionSite callSite;
        Object[] params;
        BeanImpl outcome;
        
        @Override
        public Bean hostBean() {
            return bean;
        }

        @Override
        public Method method() {
            return method;
        }

        @Override
        public Object[] params() {
            return params;
        }

        public ProducedBean outcome() {
            return outcome;
        }
        
        @Override
        public ActionSite callSite() {
            return callSite;
        }
    }
    
    private class BeanImpl implements ProducedBean {
        
        Class<?> type;
        Action producer;
        
        @Override
        public Class<?> beanType() {
            return type;
        }
        
        @Override
        public TopologyNode host() {
            return producer.host();
        }

        @Override
        public Step producer() {
            return producer();
        }
    }
    
    private class InjectedBeanImpl implements InjectedBean {

        Class<?> type;
        TopologyNode host;
        Object[] params;
        ActionSite callSite;
        
        @Override
        public Class<?> beanType() {
            return type;
        }

        @Override
        public TopologyNode host() {
            return host;
        }

        @Override
        public Object[] lookupId() {
            return params;
        }

        @Override
        public ActionSite callSite() {
            return callSite;
        }
    }
}
