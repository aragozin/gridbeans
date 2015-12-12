package org.gridkit.lab.gridbeans.monadic.builder;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.gridkit.lab.gridbeans.ActionGraph;
import org.gridkit.lab.gridbeans.ActionGraph.ActionSite;
import org.gridkit.lab.gridbeans.ActionGraph.Bean;
import org.gridkit.lab.gridbeans.ActionGraph.LocalBean;
import org.gridkit.lab.gridbeans.monadic.Checkpoint;
import org.gridkit.lab.gridbeans.monadic.ExecutionTarget;
import org.gridkit.lab.gridbeans.monadic.ExecutionGraph.CallDescription;
import org.gridkit.lab.gridbeans.monadic.ExecutionGraph.CheckpointDescription;
import org.gridkit.lab.gridbeans.monadic.ExecutionGraph.ExecutionObserver;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.BeanHandle;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.ExecutionHost;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.Invocation;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.InvocationCallback;
import org.gridkit.lab.gridbeans.monadic.builder.RawGraphData.CheckpointInfo;

class RuntimeGraph {

    private static final Method EXPORT_METHOD;
    static {
        try {
            EXPORT_METHOD = ExecutionTarget.class.getMethod("bean", Class.class, Object[].class);
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    
    private RuntimeEnvironment environment;
    private List<CheckpointState> checkpoints = new ArrayList<CheckpointState>();
    private List<Action> actions = new ArrayList<Action>();
    private List<BeanHolder> beans = new ArrayList<BeanHolder>();
    private ExecutionObserver observer;

    private int pendingCount;
    private BlockingQueue<Runnable> reactionQueue;
    private Exception failure;
    
    public RuntimeGraph(RawGraphData graph, RuntimeEnvironment environment) {
        this.environment = environment;
        new GraphProcessor(graph).process();
    }

    public void run(ExecutionObserver observer) {
        initState();
        this.observer = observer != null ? observer : new NullObserver();
        start();
    }
    
    private void start() {
        pendingCount = 0;
        reactionQueue = new ArrayBlockingQueue<Runnable>(actions.size() + beans.size());
        for(CheckpointState cs: checkpoints) {
            if (cs.seqNo == 0) {
                completed(cs);
                break;
            }
        }

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
        for(Action a: actions) {
            if (!a.completed) {
                return false;
            }
        }
        return true;
    }

    protected void completed(CheckpointState cs) {
        cs.passed = true;
        observer.onCheckpoint(new ChDescr(cs));
        for(Action a: cs.dependents) {
            reviewAction(a);
        }        
    }

    protected void completed(Action action) {
        observer.onComplete(new CallDescr(action));
        for(CheckpointState cs: action.dependents) {
            reviewCheckpoint(cs);
        }        
    }
    
    protected void available(BeanHolder holder) {
        for(Action action: holder.dependents) {
            reviewAction(action);
        }
    }

    private void reviewAction(Action a) {
        if (!a.started) {
            if (isExecutionReady(a)) {
                prepareDeps(a);
                if (isDataReady(a)) {
                    doFire(a);
                }
            }
        }
    }

    private void reviewCheckpoint(CheckpointState cs) {
        if (!cs.passed) {
            if (isExecutionReady(cs)) {
                completed(cs);
            }
        }
    }
    
    private void prepareDeps(Action a) {
        for(BeanHolder b: a.beanDeps) {
            if (b.lookupId != null) {
                if (!b.requested) {
                    doExport(b);
                }
            }
        }        
    }

    private void doExport(final BeanHolder b) {
        ++pendingCount;
        b.requested = true;        
        b.host.resolveBean(b.lookupId.lookupType, b.lookupId.lookupId, new DefereCallback() {
            
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

    private void doFire(final Action action) {
        ++pendingCount;
        action.started = true;
        observer.onFire(new CallDescr(action));
        BeanHandle handle = action.hostBean.handle;
        handle.fire(action.toInvocation(), new DefereCallback() {
            
            @Override
            public void onError(Exception error) {
                action.error = error;
                observer.onComplete(new CallDescr(action));
                failExecution(error);                
            }
            
            @Override
            public void onDone(BeanHandle handle) {
                action.completed = true;
                if (action.outputBean != null) {
                    action.outputBean.handle = handle;
                }
                completed(action);          
                if (action.outputBean != null) {
                    available(action.outputBean);
                }
            }
        });
    }
    
    private boolean isExecutionReady(Action a) {
        for(CheckpointState cs: a.checkpointDeps) {
            if (!cs.passed) {
                return false;
            }
        }
        
        for(BeanHolder b: a.beanDeps) {
            if (b.producerAction != null) {
                if (!b.producerAction.completed) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isExecutionReady(CheckpointState cs) {
        for(Action a: cs.dependencies) {
            if (!a.completed) {
                return false;
            }
        }
        return true;
    }

    private boolean isDataReady(Action a) {
        for(BeanHolder b: a.beanDeps) {
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
    
    private void initState() {
        for(Action a: actions) {
            a.completed = false;
            a.error = null;
            a.started = false;
        }
        for(BeanHolder b: beans) {
            b.handle = null;
            b.requested = false;
        }
        for(CheckpointState cs: checkpoints) {
            cs.passed = false;
        }
    }

    private class GraphProcessor {
        
        private ActionGraph graph;
        private Map<ActionGraph.LocalBean, ProtoBean> exportedBeans = new HashMap<ActionGraph.LocalBean, RuntimeGraph.ProtoBean>();
        private Map<ActionGraph.Action, ProtoAction> protoActions = new LinkedHashMap<ActionGraph.Action, RuntimeGraph.ProtoAction>();
        
        private ActionGraph.Bean omniLocator;
        private ActionGraph.Bean rootLocator;
        private RawGraphData.CheckpointInfo[] sourceGraph;
        private Set<ActionGraph.Bean> locations;
        private Map<ActionGraph.Bean, ExecutionHost[]> resolved = new LinkedHashMap<ActionGraph.Bean, ExecutionHost[]>();
        
        private int changeCounter = 0;
        private Set<Object> processed = new HashSet<Object>(); // used for actions and beans
        
        public GraphProcessor(RawGraphData graph) {
            this.sourceGraph = graph.checkpoints;
            this.omniLocator = graph.omniLocator;
            this.rootLocator = graph.rootLocator;
            this.locations = new LinkedHashSet<ActionGraph.Bean>(Arrays.asList(graph.locations));
            this.locations.add(omniLocator);
            this.locations.add(rootLocator);
        }

        void process() {
            initCheckpoints();
            if (graph == null) {
                throw new IllegalArgumentException("Execution graph is empty");
            }
            enumActions();
            resolveLocators();
            resolveLocations();
            processCheckpoints();
            processActions();
            initDependencies();
        }

        protected void initCheckpoints() {
            for(CheckpointInfo ch: sourceGraph) {
                CheckpointState cs = new CheckpointState();
                checkpoints.add(cs);
                cs.seqNo = ch.id;
                cs.name = ch.name;
                cs.site = ch.site;
                if (graph == null) {
                    if (ch.dependents.length > 0) {
                        graph = ch.dependents[0].getGraph();
                    }
                    if (ch.dependencies.length > 0) {
                        graph = ch.dependencies[0].getGraph();
                    }
                }
            }
        }        
        
        protected void enumActions() {
            for(CheckpointInfo ch: sourceGraph) {
                for(ActionGraph.Action a: ch.dependencies) {
                    initProtoAction(a);
                }
            }
        }

        private void initProtoAction(ActionGraph.Action a) {
            if (protoActions.containsKey(a)) {
                return;
            }
            else {
                ProtoAction pa = new ProtoAction();
                pa.action = a;
                protoActions.put(a, pa);
                ActionGraph.Bean host = a.getHostBean();
                if (locations.contains(host)) {
                    throw new IllegalArgumentException("Broken graph, action on locator: " + a);
                }
                initProtoBean(host);
                for(ActionGraph.Bean ab: a.getBeanParams()) {
                    if (ab != null) {
                        initProtoBean(ab);
                    }
                }
            }            
        }

        protected void initProtoBean(ActionGraph.Bean bean) {
            ActionGraph.LocalBean lbean = (LocalBean) bean;
            if (isExported(lbean)) {
                initExportedBean(lbean);
            }
            else {
                initProtoAction(lbean.getOrigin());
            }
        }

        private boolean isExported(ActionGraph.LocalBean bean) {
            ActionGraph.Action a = bean.getOrigin();
            Bean hostBean = a.getHostBean();
            return (locations.contains(hostBean) || omniLocator == hostBean || rootLocator == hostBean)
                    && a.getSite().allMethodAliases().contains(EXPORT_METHOD);
        }
        
        private void initExportedBean(ActionGraph.LocalBean bean) {
            if (!exportedBeans.containsKey(bean)) {
                ProtoBean pb = new ProtoBean();
                pb.bean = bean;
                Bean location = bean.getOrigin().getHostBean();
                pb.locators.add(location);
                if (!locations.contains(location)) {
                    throw new RuntimeException("Unknown location");
                }
                exportedBeans.put(bean, pb);
                if (location != omniLocator) {
                    resolved.put(location, null); // schedule resolution
                }
            }
        }
        
        private void resolveLocators() {
            resolved.put(rootLocator, new ExecutionHost[]{environment.root()});
            while(true) {
                int nUnresolved = 0;
                int nResolved = 0;
                for(ActionGraph.Bean locator: resolved.keySet()) {
                    if (resolved.get(locator) == null) {
                        if (tryResolve(locator)) {
                            ++nResolved;
                        }
                        else {
                            ++nUnresolved;
                        }
                    }
                }
                
                if (nUnresolved == 0) {
                    break;
                }
                if (nResolved == 0) {
                    throw new RuntimeException("Cannot resolve execution locations");
                }
            }
        }

        private boolean tryResolve(Bean locator) {
            ActionGraph.LocalBean lbean = (LocalBean) locator;
            ActionGraph.Action li = lbean.getOrigin();
            ActionGraph.Bean midhost = li.getHostBean();
            ActionGraph.Bean host = ((LocalBean)midhost).getOrigin().getHostBean();
            ExecutionHost[] eh = resolved.get(host);
            if (eh != null) {
                Class<?> lclass = midhost.getType();
                Method m = li.getSite().getMethod(lclass);
                Object[] params = li.getGroundParams();
                
                Set<ExecutionHost> th = new LinkedHashSet<RuntimeEnvironment.ExecutionHost>();
                for (ExecutionHost h: eh) {
                    for(ExecutionHost sl: h.resolveLocator(m, params)) {
                        th.add(sl);
                    }
                }
                
                if (th.isEmpty()) {
                    throw new IllegalArgumentException("Location is resolved to empty set");
                }
                
                resolved.put(locator, th.toArray(new ExecutionHost[th.size()]));
                return true;
            }
            else {
                return false;
            }
        }

        private void resolveLocations() {
            while(true) {
                changeCounter = 0;
                int nUnresolved = 0;
                
                for(ProtoAction pa: protoActions.values()) {
                    pa.locators.size();
                    tryLocalize(pa);
                    if (pa.locators.isEmpty() || (pa.locators.size() == 1 && pa.locators.contains(omniLocator))) {
                        ++nUnresolved;
                    }
                }
                
                if (nUnresolved == 0 && changeCounter == 0) {
                    break;
                }
                if (changeCounter == 0) {
                    throw new RuntimeException("Cannot resolve execution locations");
                }                
            }
        }

        private void tryLocalize(ProtoAction pa) {
            ActionGraph.Action a = pa.action;
            ActionGraph.Bean b = a.getHostBean();
            propagateLocation(b, pa);
            for(ActionGraph.Bean ab: a.getBeanParams()) {
                if (ab != null) {
                    inferLocation(ab, pa);
                }
            }
        }

        private void inferLocation(Bean b, ProtoAction pa) {
            if (exportedBeans.containsKey(b)) {
                ProtoBean pb = exportedBeans.get(b);
                if (pb.locators.contains(omniLocator)) {
                    // for omnilocated bean pull up locations from actions
                    if (pb.locators.addAll(pa.locators)) {
                        ++changeCounter;
                    }
                }                                
            }
            else {
                ActionGraph.Action sa = ((ActionGraph.LocalBean)b).getOrigin();
                ProtoAction spa = protoActions.get(sa);
                if (spa == null) {
                    new String();
                }
                if (spa.locators.contains(omniLocator)) {
                    if (spa.locators.addAll(pa.locators)) {
                        ++changeCounter;
                    }
                }
            }            
        }
        
        private void propagateLocation(Bean b, ProtoAction pa) {
            if (exportedBeans.containsKey(b)) {
                ProtoBean pb = exportedBeans.get(b);
                // else assign locations from bean to action
                if (!pb.locators.containsAll(pa.locators)) {
                    throw new RuntimeException("Internal error: cannot narrow location");
                }
                if (pa.locators.addAll(pb.locators)) {
                    ++changeCounter;
                }
            }
            else {
                ActionGraph.Action sa = ((ActionGraph.LocalBean)b).getOrigin();
                ProtoAction spa = protoActions.get(sa);
                if (spa == null) {
                    new String();
                }
                if (!spa.locators.containsAll(pa.locators)) {
                    throw new RuntimeException("Internal error: cannot narrow location");
                }
                if (pa.locators.addAll(spa.locators)) {
                    ++changeCounter;
                }
            }            
        }
        
        private void processCheckpoints() {
            for(CheckpointInfo ci: sourceGraph) {
                CheckpointState cs = lookupCheckpoint(ci.id);
                for(ActionGraph.Action a: ci.dependencies) {
                    ProtoAction pa = protoActions.get(a);
                    pa.postconds.add(cs);
                }
                for(ActionGraph.Action a: ci.dependents) {
                    ProtoAction pa = protoActions.get(a);
                    pa.preconds.add(cs);
                }
            }            
        }

        private CheckpointState lookupCheckpoint(int id) {
            for(CheckpointState cs: checkpoints) {
                if (cs.seqNo == id) {
                    return cs;
                }
            }
            throw new RuntimeException();
        }

        private void processActions() {
            for(ProtoAction pa: protoActions.values()) {
                processAction(pa);
            }                
        }

        private void processAction(ProtoAction pa) {
            if (!processed.contains(pa)) {
                processed.add(pa);
                Set<ExecutionHost> targets = targetsFor(pa.locators);
                if (targets.isEmpty()) {
                    throw new RuntimeException();
                }
                ActionGraph.Bean b = pa.action.getHostBean();
                ensureBean(b);
                for(ActionGraph.Bean ab: pa.action.getBeanParams()) {
                    if (ab != null) {
                        ensureBean(ab);
                    }
                }
                for(ExecutionHost host: targets) {
                    Object[] groundArgs = pa.action.getGroundParams();
                    BeanHolder[] beanArgs = new BeanHolder[groundArgs.length];
                    for(int i = 0; i != beanArgs.length; ++i) {                        
                        Bean ab = pa.action.getBeanParams()[i];
                        if (ab != null) {
                            BeanHolder bh = resolveBean(host, (LocalBean) ab);
                            beanArgs[i] = bh;
                        }
                    }
                    
                    BeanHolder hostBean = resolveBean(host, (LocalBean) b);
                    Action a = createAction(host, hostBean, pa.action, groundArgs, beanArgs);
                    Bean outBean = pa.action.getResultBean();
                    if (outBean != null && !outBean.getGraph().allConsumers(outBean).isEmpty()) {
                        BeanHolder oh = createProducedBeanHolder(host, outBean.getType(), a);
                        a.outputBean = oh;
                    }
                    for(CheckpointState ca: pa.postconds) {
                        ca.dependencies.add(a);
                    }
                    for(CheckpointState ca: pa.preconds) {
                        ca.dependents.add(a);
                    }
                }
            }
        }

        private BeanHolder resolveBean(ExecutionHost host, LocalBean ab) {
            if (exportedBeans.containsKey(ab)) {
                BeanHolder bh = lookupExportedBeanHolder(host, new BeanIdentity(ab.getOrigin()));
                return bh;
            }
            else {
                ActionGraph.Action prd = ab.getOrigin();
                Action aprd = lookupAction(host, prd);
                return aprd.outputBean;
            }
        }

        private void ensureBean(Bean b) {
            ProtoBean pb = exportedBeans.get(b);
            if (pb != null) {
                processBean(pb);
            }
            else {
                ActionGraph.Action pa = ((ActionGraph.LocalBean)b).getOrigin();
                processAction(protoActions.get(pa));
            }            
        }

        private void processBean(ProtoBean pb) {
            if (!processed.contains(pb)) {
                processed.add(pb);
                Set<ExecutionHost> targets = targetsFor(pb.locators);
                if (targets.isEmpty()) {
                    throw new RuntimeException();
                }
                for(ExecutionHost host: targets) {
                    createExportedBeanHolder(host, new BeanIdentity(pb.bean.getOrigin()));
                }
            }
        }

        private Set<ExecutionHost> targetsFor(Set<Bean> locators) {
            Set<ExecutionHost> targets = new LinkedHashSet<RuntimeEnvironment.ExecutionHost>();
            for(Bean locator: locators) {
                if (locator != omniLocator) {
                    for(ExecutionHost host: resolved.get(locator)) {
                        targets.add(host);
                    }
                }
            }
            
            return targets;
        }
        
        private void initDependencies() {
            for(Action a: actions) {
                for(BeanHolder bh: a.beanDeps) {
                    bh.dependents.add(a);
                }
            }
            for(CheckpointState cs: checkpoints) {
                for(Action a: cs.dependents) {
                    a.checkpointDeps.add(cs);
                }
                for(Action a: cs.dependencies) {
                    a.dependents.add(cs);
                }
            }
            
            for(Action a: actions) {
                System.out.println("ACTION " + a);
                System.out.println(" -> " + a.checkpointDeps);
                System.out.println(" -> " + a.dependents);
            }
        }
    };
    
    private static class ProtoAction {
        
        ActionGraph.Action action;
        Set<Bean> locators = new LinkedHashSet<ActionGraph.Bean>();
        
        List<CheckpointState> preconds = new ArrayList<CheckpointState>();
        List<CheckpointState> postconds = new ArrayList<CheckpointState>();
        
    }

    private static class ProtoBean {
        
        ActionGraph.LocalBean bean;
        Set<Bean> locators = new LinkedHashSet<ActionGraph.Bean>();
        
    }
    
    private static class Action {

        ActionGraph.Action source;
        
        int actionId;
        RuntimeEnvironment.ExecutionHost host;
        BeanHolder hostBean;
        Method method;
        Object[] groundParams;
        BeanHolder[] beanParams;
        BeanHolder outputBean;
        
        // Execution state
        boolean started;
        boolean completed;
        Exception error;
        
        List<CheckpointState> checkpointDeps = new ArrayList<CheckpointState>();
        List<BeanHolder> beanDeps = new ArrayList<BeanHolder>();
        List<CheckpointState> dependents = new ArrayList<CheckpointState>();
        
        public String toString() {
            return hostBean + "." + method.getName() + "()";
        }

        public Invocation toInvocation() {
            Invocation call = new Invocation(method);
            for(int i = 0; i != beanParams.length; ++i) {
                if (beanParams[i] != null) {
                    call.setBeanParam(i, beanParams[i].handle);
                }
                else {
                    call.setGroundParam(i, groundParams[i]);
                }
            }
            if (outputBean != null) {
                call.setOutputType(outputBean.beanType);
            }
            return call;
        }
    }
    
    static class CheckpointState implements Checkpoint {
        
        int seqNo;
        String name;
        StackTraceElement[] site;
        
        List<Action> dependencies = new ArrayList<Action>();
        List<Action> dependents = new ArrayList<Action>();

        boolean passed;
        
        public String toString() {
            return seqNo == 0 ? "<start>" : 
                   name == null ? "<" + seqNo + ">" : name;
        }
    }
    
    static class BeanHolder {

        int beanId;
        
        Class<?> beanType;
        RuntimeEnvironment.ExecutionHost host;
        
        // bean can be either
        // produced by action
        Action producerAction;
        // or injected by environment
        BeanIdentity lookupId;
        
        // state
        boolean requested;
        RuntimeEnvironment.BeanHandle handle;
        
        List<Action> dependents = new ArrayList<RuntimeGraph.Action>();
        
        public String toString() {
            if (producerAction != null) {
                return producerAction.toString();
            }
            else {
                return "{" + host + ":" + lookupId + "}";
            }
            
        }
    }
    
    static class BeanIdentity {
        
        Class<?> lookupType;
        Object[] lookupId;
        
        public BeanIdentity(ActionGraph.Action action) {
            this((Class<?>)action.getGroundParams()[0], (Object[])action.getGroundParams()[1]);
        }

        public BeanIdentity(Class<?> lookupType, Object[] lookupId) {
            this.lookupType = lookupType;
            this.lookupId = lookupId;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Arrays.hashCode(lookupId);
            result = prime * result + ((lookupType == null) ? 0 : lookupType.hashCode());
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
            BeanIdentity other = (BeanIdentity) obj;
            if (!Arrays.equals(lookupId, other.lookupId))
                return false;
            if (lookupType == null) {
                if (other.lookupType != null)
                    return false;
            } else if (!lookupType.equals(other.lookupType))
                return false;
            return true;
        }
        
        public String toString() {
            return lookupType.getSimpleName() + Arrays.toString(lookupId);
        }
    }

    protected BeanHolder createExportedBeanHolder(ExecutionHost host, BeanIdentity beanIdentity) {
        BeanHolder bh = new BeanHolder();
        bh.beanId = beans.size();
        bh.host = host;
        bh.beanType = beanIdentity.lookupType;
        bh.lookupId = beanIdentity;
        beans.add(bh);
        return bh;
    }

    protected BeanHolder createProducedBeanHolder(ExecutionHost host, Class<?> type, Action producer) {
        BeanHolder bh = new BeanHolder();
        bh.beanId = beans.size();
        bh.host = host;
        bh.beanType = type;
        bh.producerAction = producer;
        beans.add(bh);
        return bh;
    }
    
    protected BeanHolder lookupExportedBeanHolder(ExecutionHost host, BeanIdentity bi) {
        for(BeanHolder bh: beans) {
            if (bh.host == host && bi.equals(bh.lookupId)) {
                return bh;
            }
        }
        throw new RuntimeException("No such bean found");
    }

    protected Action createAction(ExecutionHost host, BeanHolder bean, ActionGraph.Action source, Object[] groundArgs, BeanHolder[] beanArgs) {
        Action a = new Action();
        a.actionId = actions.size();
        a.host = host;
        a.hostBean = bean;
        a.source = source;
        a.method = source.getSite().getMethod(bean.beanType);
        a.groundParams = groundArgs;
        a.beanParams = beanArgs;
        
        a.beanDeps.add(a.hostBean);
        for(BeanHolder bh: a.beanParams) {
            if (bh != null) {
                a.beanDeps.add(bh);
            }
        }
        
        actions.add(a);
        
        return a;
    }
    
    protected Action lookupAction(ExecutionHost host, ActionGraph.Action action) {
        for(Action a: actions) {
            if (a.host == host && a.source == action) {
                return a;
            }
        }
        throw new RuntimeException("No such action found");
    }
    
    private static class ChDescr implements CheckpointDescription {
        
        private CheckpointState ch;

        public ChDescr(CheckpointState ch) {
            this.ch = ch;
        }

        @Override
        public String getName() {
            return ch.toString();
        }
        
        public String toString() {
            return ch.toString();
        }
    }
    
    private static class CallDescr implements CallDescription {
        
        private Action action;

        public CallDescr(Action action) {
            this.action = action;
        }

        @Override
        public int getCallId() {
            return action.actionId;
        }

        @Override
        public Object getExecutionHost() {
            return action.host.toString();
        }

        @Override
        public Object getBeanReference() {
            return action.hostBean.toString();
        }

        @Override
        public ActionSite getCallSite() {
            return action.source.getSite();
        }

        @Override
        public String[] getParamDescription() {
            String[] params = new String[action.beanParams.length];
            for(int i = 0; i != params.length; ++i) {
                if (action.beanParams[i] != null) {
                    params[i] = action.beanParams[i].toString();
                }
                else {
                    params[i] = String.valueOf(action.groundParams[i]);
                }
            }
            return params;
        }

        @Override
        public boolean hasOutput() {
            return action.outputBean != null;
        }

        @Override
        public String getResultDescription() {
            if (action.outputBean != null) {
                return action.outputBean.toString();
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
    
    private static class NullObserver implements ExecutionObserver {

        @Override
        public void onFire(CallDescription call) {
        }

        @Override
        public void onComplete(CallDescription call) {
        }

        @Override
        public void onCheckpoint(CheckpointDescription checkpoint) {
        }

        @Override
        public void onFailure(Exception error) {
        }

        @Override
        public void onFinish() {
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
}
