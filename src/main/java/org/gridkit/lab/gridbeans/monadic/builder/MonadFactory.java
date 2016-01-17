package org.gridkit.lab.gridbeans.monadic.builder;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.gridkit.lab.gridbeans.PowerBeanProxy;
import org.gridkit.lab.gridbeans.monadic.BeanShortcut;
import org.gridkit.lab.gridbeans.monadic.BeanShortcut.BeanId;
import org.gridkit.lab.gridbeans.monadic.Checkpoint;
import org.gridkit.lab.gridbeans.monadic.DeployerSPI;
import org.gridkit.lab.gridbeans.monadic.ExecutionGraph.ExecutionObserver;
import org.gridkit.lab.gridbeans.monadic.ExecutionTarget;
import org.gridkit.lab.gridbeans.monadic.Joinable;
import org.gridkit.lab.gridbeans.monadic.Locator;
import org.gridkit.lab.gridbeans.monadic.LocatorShortcut;
import org.gridkit.lab.gridbeans.monadic.LocatorShortcut.LocationId;
import org.gridkit.lab.gridbeans.monadic.MonadBuilder;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment;
import org.gridkit.lab.gridbeans.monadic.ScenarioDefinition;
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
            DEPLOY_CALL.add(DeployerSPI.class.getMethod("deploy", Class.class, Object.class));
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static Set<Method> IMPORT_CALL = new HashSet<Method>();
    static {
        try {
            IMPORT_CALL.add(ExecutionTarget.class.getMethod("bean", Class.class, Object[].class));
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static Set<Method> EXPORT_CALL = new HashSet<Method>();
    static {
        try {
            EXPORT_CALL.add(ExecutionTarget.class.getMethod("publish", Object.class, Object[].class));
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
    private List<Bean> locators = new ArrayList<ActionGraph.Bean>();
    private CheckpointImpl start;
    
    private List<Context> stack = new ArrayList<MonadFactory.Context>();

    private int checkpointCounter = 0;

    protected MonadFactory() {
        tracker = new MonadTracker();
        omniLocation = new Location(true);
        omni = tracker.inject("omni", ExecutionTarget.class);
        rootLocation = new Location(false);
        root = tracker.inject("root", ExecutionTarget.class);
        start = new CheckpointImpl(null);
        stack.add(new Context());        
        top().rewind(start);
    }


    @Override
    public <T extends Locator> T locator(Class<T> type) {
        return root.locator(type);
    }

    @Override
    public <T> T bean(Class<T> type, Object... lookupKeys) {
        return omni.bean(type, lookupKeys);
    }
    
    @Override
    public void publish(Object bean, Object... lookupKeys) {
        omni.publish(bean, lookupKeys);
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
    public Checkpoint checkpoint(String labelId) {
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
        else {
            top().rewind((CheckpointImpl) label);
        }
    }

    Context top() {
        return stack.get(stack.size() - 1);
    }
    
    @Override
    public void sync() {
        top().join(new CheckpointImpl(true));        
    }

    @Override
    public void checkpoint() {
        top().join(new CheckpointImpl(false));        
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
    public ScenarioDefinition finish() {
        
        RawGraphData rgd = new RawGraphData();
        rgd.checkpoints = new RawGraphData.CheckpointInfo[allCheckpoints.size()];
        for(int i = 0; i != rgd.checkpoints.length; ++i) {
            CheckpointImpl ci = allCheckpoints.get(i);
            rgd.checkpoints[i] = new RawGraphData.CheckpointInfo(ci.chckId, ci.name, ci.toString(), ci.scoped, ci.dependencies, ci.dependents, ci.site);
        }
        rgd.omniLocator = tracker.proxy2bean(omni);
        rgd.rootLocator = tracker.proxy2bean(root);
        rgd.locations = locators.toArray(new ActionGraph.Bean[0]);
        
        return new MonadGraph(rgd);
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

            if (processShortcut(a)) {
                // action was transformed
                return;
            }
            else if (isLocationCall(a)) {
                validateLocationCall(a);
                locators.add(a.getResultBean());
                return;
            }
            else if (isLocatorCall(a)) {
                validateLocatorCall(a);
                return;
            }
            else if (isImportCall(a)) {
                validateImportCall(a);
            }
            else if (isExportCall(a)) {
                validateExportCall(a);
                // threat as action
                top().addAction(a);
            }
            else {
                if (isDeployCall(a)) {
                    validateDeployCall(a);
                }
                
                // Normal action, track checkpoint dependencies
                top().addAction(a);
            }
        }

        private boolean processShortcut(Action a) {
            try {
                Method m = findAnnotatedMethod(a.getSite());
                if (m != null) {
                    if (m.getAnnotation(LocatorShortcut.class) != null) {
                        a = shortcutLocator(m.getAnnotation(LocatorShortcut.class), m, a);
                    }
                    if (m.getAnnotation(BeanShortcut.class) != null) {
                        shortcutBean(m.getAnnotation(BeanShortcut.class), m, a);
                    }
                    return true;
                }
                else {
                    return false;
                }
            } catch (InvocationTargetException e) {
                if (e.getTargetException() instanceof RuntimeException) {
                    throw (RuntimeException)e.getTargetException();
                }
                else {
                    throw new RuntimeException(e.getTargetException());
                }
            
            } catch (Exception e) {
                throw new RuntimeException("Error processing annotations on " + a, e);
            }
        }

        private Action shortcutLocator(LocatorShortcut scut, Method m, Action a) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
            Class<? extends Locator> type = scut.locatorType();
            if (!type.isInterface()) {
                throw new RuntimeException("Interface required: " + type.getName());
            }
            String method = scut.method();
            List<Class<?>> locSig = new ArrayList<Class<?>>();
            List<Object> locParams = new ArrayList<Object>();
            collectParams(a, m, locSig, locParams, LocatorShortcut.LocationId.class);

            Class<?>[] sig = locSig.toArray(new Class<?>[locSig.size()]);
            Object[] args = locParams.toArray(new Object[locParams.size()]);
            
            ExecutionTarget host = (ExecutionTarget) bean2proxy(a.getHostBean());
            Locator lt = host.locator(type);
            Method mc = type.getMethod(method, sig);
            mc.setAccessible(true);
            Object location = mc.invoke(lt, args);
            Bean lbean = proxy2bean(location);
            getGraph().unify(a, lbean);
            
            return ((LocalBean)lbean).getOrigin();
        }

        private void shortcutBean(BeanShortcut bcut, Method m, Action a) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
            Class<?> type = bcut.beanType();
            if (!type.isInterface()) {
                throw new RuntimeException("Interface required: " + type.getName());
            }
            String method = bcut.method();
            if (method.length() == 0) {
                method = m.getName();
            }
            List<Class<?>> bSig = new ArrayList<Class<?>>();
            List<Object> bParams = new ArrayList<Object>();
            collectParams(a, m, bSig, bParams, BeanShortcut.BeanId.class);

            List<Class<?>> cSig = new ArrayList<Class<?>>();
            List<Object> cParams = new ArrayList<Object>();
            collectParams(a, m, cSig, cParams, null);

            Object[] beanId = bParams.toArray(new Object[bParams.size()]);

            Class<?>[] sig = cSig.toArray(new Class<?>[cSig.size()]);
            Object[] args = cParams.toArray(new Object[cParams.size()]);
            
            ExecutionTarget host = (ExecutionTarget) bean2proxy(a.getHostBean());
            Object bean = host.bean(type, beanId);
            Method mc = type.getMethod(method, sig);
            mc.setAccessible(true);
            Object result = mc.invoke(bean, args);
            if (a.getSite().getMethod().equals(m)) {
                // shortcuted action should be eliminated                
                if (result != null) {
                    Bean lbean = proxy2bean(result);
                    getGraph().unify(a, lbean);
                }
                else {
                    getGraph().eliminate(a);
                }
            }
        }
        
        private void collectParams(Action a, Method m, List<Class<?>> matchedSignature, List<Object> matchedParams, Class<? extends Annotation> anno) {
            Annotation[][] pa = m.getParameterAnnotations();
            Class<?>[] p = m.getParameterTypes();
            for(int i = 0; i != p.length; ++i) {
                if (match(pa[i], anno)) {
                    addParam(a, matchedSignature, matchedParams, p, i);
                }
            }
        }

        protected void addParam(Action a, List<Class<?>> matchedSignature, List<Object> matchedParams, Class<?>[] p, int i) {
            matchedSignature.add(p[i]);
            if (a.getBeanParams()[i] != null) {
                matchedParams.add(bean2proxy(a.getBeanParams()[i]));
            }
            else {
                matchedParams.add(a.getGroundParams()[i]);
            }
        }

        private boolean match(Annotation[] annotations, Class<? extends Annotation> anno) {
            if (anno == null) {
                for(Annotation a: annotations) {
                    if (a instanceof BeanId || a instanceof LocationId) {
                        return false;
                    }
                }                
                return true;
            }
            else {
                for(Annotation a: annotations) {
                    if (anno.isInstance(a)) {
                        return true;
                    }
                }
                return false;
            }
        }

        private Method findAnnotatedMethod(ActionSite site) {
            Method m = null;
            for(Method x: site.allMethodAliases()) {
                if ((x.getAnnotation(LocatorShortcut.class) != null)||(x.getAnnotation(BeanShortcut.class) != null)) {
                    if (m == null) {
                        m = x;
                    }
                    else {
                        throw new RuntimeException("Ambigous annotations for method [" + site.getMethod().getName() + "]");
                    }
                }
            }
            return m;
        }

        private boolean isLocatorCall(Action a) {
            for(Method m: LOCATOR_CALL) {
                if (a.getSite().allMethodAliases().contains(m)) {
                    return true;
                }
            }
            return false;
        }

        private void validateLocatorCall(Action a) {
            // no validation            
        }

        private boolean isLocationCall(Action a) {            
            return isLocator(a.getHostBean());
        }

        private void validateLocationCall(Action a) {
            // Location (scope) calls is not part of graph
            // Additional contract is enforced
            Bean result = a.getResultBean();
            if (result == null || !ExecutionTarget.class.isAssignableFrom(result.getType())) {
                throw new IllegalArgumentException("Locator contract violation, call MUST return ExecutionTarget");
            }
            for(Bean b: a.getBeanParams()) {
                if (b != null) {
                    throw new IllegalArgumentException("Locator contract violation, locator cannot accept bean references as arguments");
                }
            }
        }

        private boolean isImportCall(Action a) {
            for(Method m: IMPORT_CALL) {
                if (a.getSite().allMethodAliases().contains(m)) {
                    return true;
                }
            }
            return false;
        }

        private void validateImportCall(Action a) {
            // Import call is special case
            // Extra validation applies          
            
            Bean result = a.getResultBean();
            if (result == null) {
                Class<?> type = (Class<?>) a.getGroundParams()[0];
                if (type != null && !type.isInterface()) {
                    throw new IllegalArgumentException("Bean type should be an interface, but class is passed [" + type.getName() + "]");
                }
                else {
                    throw new IllegalArgumentException("Deployment failed, please verify parameters");
                }
            }
            Object[] varArg = (Object[]) a.getGroundParams()[1];
            if (varArg != null) {
                for(Object o: varArg) {
                    if (PowerBeanProxy.getHandler(o) != null) {
                        throw new IllegalArgumentException("Beans cannot be used as lookup keys");
                    }
                }
            }
        }

        private boolean isExportCall(Action a) {
            for(Method m: EXPORT_CALL) {
                if (a.getSite().allMethodAliases().contains(m)) {
                    return true;
                }
            }
            return false;
        }

        private void validateExportCall(Action a) {
            // Export call is special case
            // Extra validation applies          
            
            Object bean = a.getBeanParams()[0];
            if (bean == null) {
                throw new IllegalArgumentException("First argument should be bean created by this builder");
            }
            Object[] varArg = (Object[]) a.getGroundParams()[1];
            if (varArg != null) {
                for(Object o: varArg) {
                    if (PowerBeanProxy.getHandler(o) != null) {
                        throw new IllegalArgumentException("Beans cannot be used as lookup keys");
                    }
                }
            }
        }
        
        private boolean isDeployCall(Action a) {
            for(Method m: DEPLOY_CALL) {
                if (a.getSite().allMethodAliases().contains(m)) {
                    return true;
                }
            }
            return false;
        }

        private void validateDeployCall(Action a) {
            // deploy(...) is a regular action, 
            // but extra validation applies 
            
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
        boolean scoped = false;
        StackTraceElement[] site;
        
        List<ActionGraph.Action> dependencies = new ArrayList<ActionGraph.Action>();
        List<ActionGraph.Action> dependents = new ArrayList<ActionGraph.Action>();
        

        public CheckpointImpl(boolean scoped) {
            // TODO trim
            this.site = trimStakcTrace(Thread.currentThread().getStackTrace());
            this.scoped = scoped;
        }

        public CheckpointImpl(String name) {
            this.site = trimStakcTrace(Thread.currentThread().getStackTrace());
            this.name = name;
        }

        public String toString() {
            return chckId == 0 ? "<start>" : 
                   name == null ? "< #" + chckId + callSite() + " >" : name;
        }
        
        private String callSite() {
            if (site != null && site.length > 0) {
                String cn = site[0].getClassName();
                if (cn.lastIndexOf('.') > 0) {
                    cn = cn.substring(cn.lastIndexOf('.') + 1, cn.length());
                }
                String st = " @ " + cn + "." + site[0].getMethodName();
                if (site[0].getFileName() != null) {
                    st += "(" + site[0].getFileName();
                    if (site[0].getLineNumber() >= 0) {
                        st += ":" + site[0].getLineNumber();
                    }
                    st += ")";
                }
                return st;
            }
            else {
                return "";
            }
        }

        private StackTraceElement[] trimStakcTrace(StackTraceElement[] stackTrace) {
            int n = 1;
            for(; n <= stackTrace.length; ++n) {
                String cn = stackTrace[n].getClassName();
                if (cn.equals(MonadFactory.class.getName())
                    ||cn.startsWith(MonadFactory.class.getName() + "$")) {
                    continue;
                }
                else {
                    break;
                }
            }
            return Arrays.copyOfRange(stackTrace, n, stackTrace.length);
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
    
    private static class MonadGraph implements ScenarioDefinition {
        
        private RawGraphData graphData;
        
        public MonadGraph(RawGraphData graphData) {
            this.graphData = graphData;
        }

        @Override
        public ExecutionClosure bind(RuntimeEnvironment environment) {
            final RuntimeGraph rg = new RuntimeGraph(graphData, environment);
            return new ExecutionClosure() {
                
                @Override
                public void execute(ExecutionObserver observer) {
                    rg.run(observer);
                }
            };
        }
    }
}
