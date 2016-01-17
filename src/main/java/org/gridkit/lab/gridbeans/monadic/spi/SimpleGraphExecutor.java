package org.gridkit.lab.gridbeans.monadic.spi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.gridkit.lab.gridbeans.ActionGraph.ActionSite;
import org.gridkit.lab.gridbeans.monadic.ExecutionGraph;
import org.gridkit.lab.gridbeans.monadic.ExecutionGraph.Bean;
import org.gridkit.lab.gridbeans.monadic.ExecutionGraph.CallDescription;
import org.gridkit.lab.gridbeans.monadic.ExecutionGraph.CallStep;
import org.gridkit.lab.gridbeans.monadic.ExecutionGraph.Checkpoint;
import org.gridkit.lab.gridbeans.monadic.ExecutionGraph.ExecutionObserver;
import org.gridkit.lab.gridbeans.monadic.ExecutionGraph.InjectedBean;
import org.gridkit.lab.gridbeans.monadic.ExecutionGraph.ProducedBean;
import org.gridkit.lab.gridbeans.monadic.ExecutionGraph.PublishStep;
import org.gridkit.lab.gridbeans.monadic.ExecutionGraph.Start;
import org.gridkit.lab.gridbeans.monadic.ExecutionGraph.Step;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.BeanHandle;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.ExecutionHost;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.Invocation;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.InvocationCallback;
import org.gridkit.lab.gridbeans.monadic.RuntimeTopology;

public class SimpleGraphExecutor implements Runnable {

    private Map<Step, ActionState> actionState = new HashMap<Step, ActionState>();
    private Map<Bean, BeanState> beanState = new HashMap<Bean, BeanState>();

    private RuntimeEnvironment environment;
    private ExecutionGraph graph;
    private ExecutionObserver observer;

    private int pendingCount;
    private BlockingQueue<Runnable> reactionQueue;
    private Exception failure;
    
    public SimpleGraphExecutor(RuntimeEnvironment env, ExecutionGraph graph, ExecutionObserver observer) {
        this.environment = env;
        this.graph = graph;
        this.observer = observer;
        initStep(graph);        
    }
    
    public void run() {
        start();
        if (failure != null) {
            throw new RuntimeException("Graph execution failure", failure);
        }
    }

    protected void initStep(ExecutionGraph graph) {
        for(Step step: graph.allSteps()) {
            ActionState state = new ActionState(actionState.size() + 1, step);
            actionState.put(step, state);
            if (step instanceof CallStep) {
                initBean(state, ((CallStep) step).hostBean());
                for(Object p: ((CallStep) step).params()) {
                    if (p instanceof Bean) {
                        initBean(state, (Bean) p);
                    }
                }
            }
            else if (step instanceof PublishStep) {
                initBean(state, ((PublishStep) step).bean());
            }
        }
    }
    
    private void initBean(ActionState astate, Bean bean) {
        if (beanState.containsKey(bean)) {
            beanState.put(bean, new BeanState(bean));
        }
        astate.beanDeps.add(beanState.get(bean));
        beanState.get(bean).denepdents.add(astate);
    }

    private void start() {
        pendingCount = 0;
        reactionQueue = new ArrayBlockingQueue<Runnable>(actionState.size() + beanState.size());
        completed(graph.start());

        try {
            loop();
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted");
        }
        if (failure != null) {
            if (failure instanceof RuntimeException) {
                throw (RuntimeException)failure;
            }
            else {
                throw new RuntimeException(failure);
            }
        }
    }

    private void loop() throws InterruptedException {
        while(failure == null) {
            List<Runnable> buf = new ArrayList<Runnable>();
            reactionQueue.drainTo(buf);
            for(Runnable r: buf) {
                r.run();
            }
            if (pendingCount == 0) {
                if (isFinished()) {
                    finish();
                }
                else {
                    failExecution(new RuntimeException("Deadlock"));
                }
                break;
            }
            reactionQueue.take().run();
        }
    }

    private void finish() {
        observer.onFinish();       
    }

    private boolean isFinished() {
        for(ActionState a: actionState.values()) {
            if (!a.completed) {
                return false;
            }
        }
        return true;
    }

    protected void completed(Step cs) {
        if (!(cs instanceof Start)) {
            actionState.get(cs).completed = true;
        }
        if (cs instanceof Checkpoint) {
            observer.onCheckpoint((Checkpoint) cs);
        }
        else if (cs instanceof CallStep) {
            observer.onComplete(callDescriptor(actionState.get(cs)));
        }
        for(Step a: cs.dependents()) {
            reviewAction(a);
        }        
    }

    protected void available(BeanState holder) {
        for(ActionState action: holder.denepdents) {
            reviewAction(action.step);
        }
    }

    private void reviewAction(Step step) {
        ActionState a = actionState.get(step);
        if (!a.started) {
            if (isExecutionReady(a)) {
                prepareDeps(a);
                if (isDataReady(a)) {
                    doFire(a);
                }
            }
        }
    }

    private void prepareDeps(ActionState a) {
        for(BeanState b: a.beanDeps) {
            if (b.bean instanceof InjectedBean) {
                if (!b.requested) {
                    doExport(b);
                }
            }
        }        
    }

    private void doExport(final BeanState b) {
        ++pendingCount;
        b.requested = true;
        InjectedBean ib = (InjectedBean) b.bean;
        ExecutionHost host = environment.lookupHost(ib.host());
        host.resolveBean(ib.beanType(), ib.lookupId(), new DefereCallback() {
            
            @Override
            public void onError(Exception error) {
                failExecution(new RuntimeException("Failed to resove bean " + b, error));
            }
            
            @Override
            public void onDone(BeanHandle handle) {
                b.handle = handle;
                available(b);                
            }
        });
    }

    private void doFire(final ActionState action) {
        ++pendingCount;
        action.started = true;
        if (action.step instanceof CallStep) {
            observer.onFire(callDescriptor(action));
            BeanHandle handle = beanState.get(((CallStep)action.step).hostBean()).handle;
            handle.fire(toInvocation(action), new DefereCallback() {
                
                @Override
                public void onError(Exception error) {
                    action.error = error;
                    observer.onComplete(callDescriptor(action));
                    failExecution(error);                
                }
                
                @Override
                public void onDone(BeanHandle handle) {
                    action.completed = true;
                    ProducedBean outcome = ((CallStep)action.step).outcome();
                    if (outcome != null) {
                        beanState.get(outcome).handle = handle;
                    }
                    completed(action.step);          
                    if (outcome != null) {
                        available(beanState.get(outcome));
                    }
                }
            });
        }
        else if (action.step instanceof PublishStep) {
            
            final Bean out = ((PublishStep)action.step).bean();
            
            reactionQueue.add(new Runnable() {                
                @Override
                public void run() {
                    --pendingCount;
                    action.completed = true;

                    action.completed = true;
                    Bean outcome = ((PublishStep)action.step).outcome();
                    if (outcome != null) {
                        beanState.get(outcome).handle = beanState.get(out).handle;
                    }
                    completed(action.step);          
                    if (outcome != null) {
                        available(beanState.get(outcome));
                    }
                }
            });
        }
        else {
            // TODO
        }
    }
    
    private boolean isExecutionReady(ActionState a) {
        for(Step cs: a.step.dependencies()) {
            if (!isCompleted(cs)) {
                return false;
            }
        }
        
        for(BeanState b: a.beanDeps) {
            if (b.bean instanceof ProducedBean) {
                if (b.handle == null) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isCompleted(Step cs) {
        return actionState.get(cs).completed;
    }

    private boolean isDataReady(ActionState a) {
        for(BeanState b: a.beanDeps) {
            if (b.handle == null) {
                return false;
            }
        }
        return true;
    }

    private void failExecution(Exception e) {
        observer.onFailure(e);
        failure = e;
    }
    
    private Invocation toInvocation(ActionState astate) {
        CallStep cs = (CallStep) astate.step;
        Invocation call = new Invocation(cs.method());
        Object[] params = cs.params();
        for(int i = 0; i != params.length; ++i) {
            if (params[i] instanceof Bean) {
                call.setBeanParam(i, beanState.get(params[i]).handle);
            }
            else {
                call.setGroundParam(i, params[i]);
            }
        }
        if (cs.outcome() != null) {
            call.setOutputType(cs.outcome().beanType());
        }
        return call;
    }
    
    private CallDescription callDescriptor(ActionState cs) {
        return new CallDescr(cs);
    }
    
    private static class ActionState {
        
        int id;
        Step step;
        boolean started;
        boolean completed;
        Exception error;

        List<BeanState> beanDeps = new ArrayList<BeanState>();
        
        public ActionState(int id, Step step) {
            this.step = step;
        }
    }
    
    private static class BeanState {

        Bean bean;
        boolean requested;
        BeanHandle handle;
        List<ActionState> denepdents = new ArrayList<ActionState>();

        public BeanState(Bean bean) {
            this.bean = bean;
        }
    }
    
    private abstract class DefereCallback implements InvocationCallback {

        @Override
        public final void done(final BeanHandle handle) {
            try {
                reactionQueue.put(new Runnable() {
                    @Override
                    public void run() {
                        --pendingCount;
                        onDone(handle);
                    }
                });
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }            
        }

        @Override
        public final void error(final Exception error) {
            try {
                reactionQueue.put(new Runnable() {
                    @Override
                    public void run() {
                        --pendingCount;
                        onError(error);
                    }
                });
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }            
        }
        
        public abstract void onDone(BeanHandle handle);

        public abstract void onError(Exception error);     
    }
    
    private static class CallDescr implements CallDescription {
        
        private ActionState action;

        public CallDescr(ActionState action) {
            this.action = action;
        }

        @Override
        public int getCallId() {
            return action.id;
        }

        @Override
        public RuntimeTopology.TopologyNode getExecutionHost() {
            return action.step.host();
        }

        @Override
        public Object getBeanReference() {
            return ((CallStep)action.step).hostBean().toString();
        }

        @Override
        public ActionSite getCallSite() {
            return action.step.callSite();
        }

        @Override
        public String[] getParamDescription() {
            CallStep cs= (CallStep) action.step;
            String[] params = new String[cs.params().length];
            for(int i = 0; i != params.length; ++i) {
                params[i] = String.valueOf(cs.params()[i]);
            }
            return params;
        }

        @Override
        public boolean hasOutput() {
            return ((CallStep)action.step).outcome() != null;
        }

        @Override
        public String getResultDescription() {
            Bean o = ((CallStep)action.step).outcome();
            if (o != null) {
                return o.toString();
            }
            else {
                return null;
            }
        }

        @Override
        public Throwable getException() {
            return action.error;
        }
        
        public String toString() {
            return action.toString();
        }
    }    
}
