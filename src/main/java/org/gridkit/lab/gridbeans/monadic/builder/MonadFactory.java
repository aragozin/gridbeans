package org.gridkit.lab.gridbeans.monadic.builder;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gridkit.lab.gridbeans.ActionGraph;
import org.gridkit.lab.gridbeans.ActionGraph.Action;
import org.gridkit.lab.gridbeans.ActionGraph.ActionSite;
import org.gridkit.lab.gridbeans.ActionGraph.Bean;
import org.gridkit.lab.gridbeans.ActionGraph.LocalBean;
import org.gridkit.lab.gridbeans.ActionTracker;
import org.gridkit.lab.gridbeans.monadic.Checkpoint;
import org.gridkit.lab.gridbeans.monadic.ExecutionTarget;
import org.gridkit.lab.gridbeans.monadic.Joinable;
import org.gridkit.lab.gridbeans.monadic.Locator;
import org.gridkit.lab.gridbeans.monadic.Monad;
import org.gridkit.lab.gridbeans.monadic.MonadBuilder;
import org.gridkit.lab.gridbeans.monadic.Wallclock;

public class MonadFactory implements MonadBuilder {

    private static Set<Method> LOCATOR_CALL = new HashSet<Method>();
    static {
        try {
            LOCATOR_CALL.add(ExecutionTarget.class.getMethod("locator", Class.class));
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static Set<Method> DEPLOY_CALL = new HashSet<Method>();
    static {
        try {
            DEPLOY_CALL.add(ExecutionTarget.class.getMethod("deploy", Class.class, Object.class));
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static MonadBuilder build() {
        return new MonadFactory();
    }

    private MonadTracker tracker;
    private Location omniLocation;
    private ExecutionTarget omni;
    private Location rootLocation;
    private ExecutionTarget root;
    private Map<String, Checkpoint> checkpoints = new HashMap<String, Checkpoint>();
    private List<CheckpointImpl> allCheckpoints = new ArrayList<CheckpointImpl>();
    private CheckpointImpl start;
    
    private List<Context> stack = new ArrayList<MonadFactory.Context>();

    private int checkpointCounter = 0;

    protected MonadFactory() {
        tracker = new MonadTracker();
        omniLocation = new Location(true);
        omni = tracker.inject(omni, ExecutionTarget.class);
        rootLocation = new Location(false);
        root = tracker.inject(root, ExecutionTarget.class);
        start = new CheckpointImpl(null);
        stack.add(new Context());        
        top().rewind(start);
    }


    @Override
    public <T extends Locator> T locator(Class<T> type) {
        return omni.locator(type);
    }

    @Override
    public <T> T bean(Class<T> type) {
        return omni.bean(type);
    }

    @Override
    public <T> T bean(Class<T> type, Object identity) {
        return omni.bean(type, identity);
    }

    
    @Override
    public <T, B extends T> T deploy(Class<T> intf, B bean) {
        return omni.deploy(intf, bean);
    }

    @Override
    public Checkpoint start() {
        return start;
    }

    @Override
    public Checkpoint label(String labelId) {
        if (labelId == null) {
            throw new NullPointerException("label ID is null");
        }
        if (checkpoints.containsKey(labelId)) {
            return checkpoints.get(labelId);
        }
        else {
            Checkpoint lb = new CheckpointImpl(labelId);
            checkpoints.put(labelId, lb);
            return lb;
        }
    }

    @Override
    public ExecutionTarget root() {
        return root;
    }

    @Override
    public void rewind(Checkpoint label) {
        if (label == null) {
            rewind(start);
        }
        else if (!(label instanceof CheckpointImpl)) {
            throw new IllegalArgumentException("Unsupported checkpoint ref: " + label); 
        }
    }

    Context top() {
        return stack.get(stack.size() - 1);
    }
    
    @Override
    public void sync() {
        top().join(new CheckpointImpl());        
    }

    private CheckpointImpl castCheckpoint(Checkpoint label) {
        return (CheckpointImpl)label;
    }
    
    @Override
    public void join(Checkpoint label) {
        top().join(castCheckpoint(label));
    }

    @Override
    public void push() {
        Context ctx = new Context();
        ctx.rewind(start);
        stack.add(ctx);
    }

    @Override
    public void pop() {
        if (stack.size() == 1) {
            throw new IllegalStateException("Cannot pop top of stack");
        }
        stack.remove(stack.size() - 1);
    }

    @Override
    public void exportAndPop() {
        Context ctx = top();
        pop();
        top().importContext(ctx);
    }

    @Override
    public Monad finish() {
        
        return null;
    }

    @Override
    public void rewind() {
        top().rewind(start);
    }

    @Override
    public void join(Joinable joinable) {
        push();
        joinable.join();
        exportAndPop();        
    }

    @Override
    public Wallclock wallclock() {
        return root().bean(Wallclock.class);
    }
    
    private class MonadTracker extends ActionTracker {

        public MonadTracker() {
        }

        @Override
        protected void afterAction(ActionSite site) {
            super.afterAction(site);
            Set<Action> actions = getGraph().allActions(site);
            if (actions.size() != 1) {
                throw new RuntimeException("Ambigous call site");
            }
            Action a = actions.iterator().next();

            if (isLocator(a.getHostBean())) {
                // Locator calls is not part of graph
                // Locator contract enforcing
                Bean result = a.getResultBean();
                if (result == null) {
                    throw new IllegalArgumentException("Locator contract violation, call MUST return ExecutionTarget");
                }
                for(Bean b: a.getBeanParams()) {
                    if (b != null) {
                        throw new IllegalArgumentException("Locator contract violation, locator cannot accept bean references as arguments");
                    }
                }
            }
            else {
                // Normal action, track checkpoint dependencies
                top().addAction(a);
            
                // Validate deploy(...) call
                for(Method m: DEPLOY_CALL) {
                    if (site.allMethodAliases().contains(m)) {
                        Bean result = a.getResultBean();
                        if (result == null) {
                            Class<?> type = (Class<?>) a.getGroundParams()[0];
                            if (type != null && !type.isInterface()) {
                                throw new IllegalArgumentException("Deploy type should be an interface, but class is passed [" + type.getName() + "]");
                            }
                            else {
                                throw new IllegalArgumentException("Deployment failed, please verify parameters");
                            }
                        }
                        if (a.getBeanParams()[1] != null) {
                            throw new IllegalArgumentException("Real object required for deployment");
                        }
                        if (a.getGroundParams()[1] == null) {
                            throw new IllegalArgumentException("null cannot be deployed");
                        }
                    }
                }
            }
        }

        private boolean isLocator(Bean hostBean) {
            if (Locator.class.isAssignableFrom(hostBean.getType())) {
                if (hostBean instanceof LocalBean) {
                    LocalBean lb = (LocalBean) hostBean;
                    for(Method m: LOCATOR_CALL) {
                        if (lb.getOrigin().getSite().allMethodAliases().contains(m)) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }
    }
    
    private static class LookupId {
        
        final Class<?> intf;
        final Object id;
        
        public LookupId(Class<?> intf, Object id) {
            this.intf = intf;
            this.id = id;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result + ((intf == null) ? 0 : intf.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            LookupId other = (LookupId) obj;
            if (id == null) {
                if (other.id != null)
                    return false;
            } else if (!id.equals(other.id))
                return false;
            if (intf == null) {
                if (other.intf != null)
                    return false;
            } else if (!intf.equals(other.intf))
                return false;
            return true;
        }
    }
    
    private static class LocatorRef {
        
        Location scope;
        LookupId lookupId;
        
    }
    
    private static class Location {
        
        LocatorRef locator;
        ActionGraph.LocalBean graphBean;
        Method method;
        Object[] parameters;
        boolean omni = false;
        
        public Location(boolean omni) {
            this(null, null, null);
            this.omni = omni;
        }

        public Location(LocatorRef locator, Method method, Object... parameters) {
            this.locator = locator;
            this.method = method;
            this.parameters = parameters;
        }

        public boolean isOmni() {
            return locator == null;
        }

        public boolean isRoot() {
            return locator != null && method == null;
        }
    }
    
    private static class ProducedBeanRef {
        
        Location scope;
        ActionGraph.LocalBean graphBean;
        
    }
    
    private static class ResovedBeanRef {
        
        Location scope;
        LookupId lookupId;
    }

    private static class ActionInfo {

        Checkpoint[] predeps;
        Location scope;
        ActionGraph.Action action;
        ProducedBeanRef resultBean;
    }

    private class CheckpointImpl implements Checkpoint {
        
        String name;
        int chckId = checkpointCounter++;
        {
            allCheckpoints.add(this);
        }
        StackTraceElement[] site;
        
        List<ActionGraph.Action> dependencies = new ArrayList<ActionGraph.Action>();
        List<ActionGraph.Action> dependents = new ArrayList<ActionGraph.Action>();
        

        public CheckpointImpl() {
            // TODO trim
            this.site = Thread.currentThread().getStackTrace();
        }

        public CheckpointImpl(String name) {
            this.name = name;
        }

        public String toString() {
            return chckId == 0 ? "<start>" : 
                   name == null ? "<" + chckId + ">" : name;
        }
    }
    
    private static class Context {
    
        private CheckpointImpl start;
        private List<ActionGraph.Action> openActions = new ArrayList<ActionGraph.Action>();
        
        public void addAction(ActionGraph.Action action) {
            start.dependents.add(action);
            openActions.add(action);
        }
        
        public void importContext(Context ctx) {
            openActions.addAll(ctx.openActions);
        }
        
        public void join(CheckpointImpl impl) {
            impl.dependencies.addAll(openActions);
            start = impl;
        }

        public void rewind(CheckpointImpl impl) {
            openActions.clear();
            start = impl;
        }
        
    }
    
    private static class MonadGraph {
        
        
    }
}
