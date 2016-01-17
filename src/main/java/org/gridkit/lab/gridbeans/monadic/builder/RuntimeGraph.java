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
import org.gridkit.lab.gridbeans.monadic.ExecutionGraph;
import org.gridkit.lab.gridbeans.monadic.ExecutionGraph.CallDescription;
import org.gridkit.lab.gridbeans.monadic.ExecutionGraph.CallStep;
import org.gridkit.lab.gridbeans.monadic.ExecutionGraph.ExecutionObserver;
import org.gridkit.lab.gridbeans.monadic.ExecutionGraph.InjectedBean;
import org.gridkit.lab.gridbeans.monadic.ExecutionGraph.PublishStep;
import org.gridkit.lab.gridbeans.monadic.ExecutionGraph.Step;
import org.gridkit.lab.gridbeans.monadic.ExecutionTarget;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.BeanHandle;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.ExecutionHost;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.Invocation;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.InvocationCallback;
import org.gridkit.lab.gridbeans.monadic.RuntimeTopology.TopologyNode;
import org.gridkit.lab.gridbeans.monadic.builder.RawGraphData.CheckpointInfo;
import org.gridkit.lab.gridbeans.monadic.spi.SimpleExecutionGraph;

class RuntimeGraph {

    private static final Method IMPORT_METHOD;
    static {
        try {
            IMPORT_METHOD = ExecutionTarget.class.getMethod("bean", Class.class, Object[].class);
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Method EXPORT_METHOD;
    static {
        try {
            EXPORT_METHOD = ExecutionTarget.class.getMethod("publish", Object.class, Object[].class);
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

    public ExecutionGraph exportGraph() {

        Map<BeanHolder, ExecutionGraph.Bean> bmap = new HashMap<RuntimeGraph.BeanHolder, ExecutionGraph.Bean>();
        Map<Action, Step> amap = new HashMap<Action, Step>();
//        Map<CheckpointState, ExecutionGraph.Checkpoint> cmap = new HashMap<CheckpointState, ExecutionGraph.Checkpoint>();
        
        SimpleExecutionGraph seg = new SimpleExecutionGraph();

        for(BeanHolder bean: beans) {
            if (bean.producerAction == null) {
                InjectedBean b = seg.inject(bean.host, bean.beanType, bean.lookupId.lookupId, null);
                bmap.put(bean, b);
            }
        }
        
        while(amap.size() < actions.size()) {
            nextAction:
            for(Action a: actions) {
                if (!amap.containsKey(a)) {
                    if (isExportAction(a)) {
                        BeanHolder bh = a.beanParams[0];
                        Object[] id = (Object[]) a.groundParams[1];
                        ExecutionGraph.Bean eb = bmap.get(bh);
                        if (eb == null) {
                            continue nextAction;
                        }
                        
                        PublishStep ps = seg.publish(eb, id, a.source.getSite());
                        amap.put(a, ps);
                        ExecutionGraph.Bean outcome = seg.produce(ps);
                        bmap.put(a.outputBean, outcome);
                    }
                    else {
                        BeanHolder bh = a.hostBean;
                        ExecutionGraph.Bean eb = bmap.get(bh);
                        if (eb == null) {
                            continue nextAction;
                        }
                        Object[] params = new Object[a.beanParams.length];
                        for(int i = 0; i != params.length; ++i) {
                            if (a.beanParams[i] != null) {
                                ExecutionGraph.Bean bp = bmap.get(a.beanParams[i]);
                                if (bp == null) {
                                    continue nextAction;
                                }
                                else {
                                    params[i] = bp;
                                }
                            }
                            else {
                                params[i] = a.groundParams[i];
                            }
                        }
                    
                        CallStep cs = seg.call(eb, a.method, params, a.source.getSite());
                        amap.put(a, cs);
                        if (a.outputBean != null) {
                            ExecutionGraph.Bean outcome = seg.produce(a.outputBean.beanType, cs);
                            bmap.put(a.outputBean, outcome);
                        }
                    }
                }
            }        
        }    
        
        for(CheckpointState cs: checkpoints) {
            if (cs.seqNo != 0) {
                // ignore start checkpoint
                
                ExecutionGraph.Checkpoint ec = seg.checkpoint(cs.name, cs.host, null);
                for(Action a: cs.dependencies) {
                    seg.depends(ec, amap.get(a));
                }
                for(Action a: cs.dependents) {
                    seg.depends(amap.get(a), ec);
                }
            }
        }     
        
        return seg;
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
        for(CheckpointState c: cs.cpDependents) {
            reviewCheckpoint(c);
        }
        for(Action a: cs.dependents) {
            reviewAction(a);
        }        
    }

    protected void completed(Action action) {
        if (!isExportAction(action)) {
            observer.onComplete(new CallDescr(action));
        }
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
            if (b.producerAction == null) {
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
        if (isExportAction(action)) {
            action.started = true;
            ++pendingCount;
            final BeanHandle out = action.beanParams[0].handle;
            
            reactionQueue.add(new Runnable() {                
                @Override
                public void run() {
                    --pendingCount;
                    action.completed = true;
                    
                    action.outputBean.handle = out;
                    completed(action);                    
                    available(action.outputBean);

                    // propagate handle
                    for(BeanHolder bh: beans) {
                        if (bh.producerAction == action) {
                            bh.handle = out;
                            bh.requested = true;
                            available(bh);
                        }
                    }
                }
            });
        }
        else {
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
    }
    
    private boolean isExportAction(Action action) {
        return action.hostBean == null && EXPORT_METHOD.equals(action.method);
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
        if (cs.cpDependency != null && !cs.cpDependency.passed) {
            return false;
        }
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
        private Map<ActionGraph.LocalBean, ProtoBean> injectedBeans = new HashMap<ActionGraph.LocalBean, RuntimeGraph.ProtoBean>();
        private Map<ActionGraph.Action, ProtoAction> protoActions = new LinkedHashMap<ActionGraph.Action, RuntimeGraph.ProtoAction>();
        
        private ActionGraph.Bean omniLocator;
        private ActionGraph.Bean rootLocator;
        private RawGraphData.CheckpointInfo[] sourceGraph;
        private Set<ActionGraph.Bean> locations;
        private Map<ActionGraph.Bean, ExecutionHost[]> resolved = new LinkedHashMap<ActionGraph.Bean, ExecutionHost[]>();
        private List<ExportRef> exports = new ArrayList<RuntimeGraph.ExportRef>();
        private Map<ExportTarget, Action> exportMap = new HashMap<RuntimeGraph.ExportTarget, RuntimeGraph.Action>();
        
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
            verifyExportedBeans();
            resolveLocators();
            resolveLocations();
            processCheckpoints();
            processActions();
            linkExports();
            initDependencies();
        }

        private void verifyExportedBeans() {
            for(ExportRef export: exports) {
                BeanIdentity bi = export.identity;
                for(ExportRef ee: exports) {
                    if (ee != export) {
                        if (bi.equals(ee.identity)) {
                            if (export.isOmniLocation() && ee.isOmniLocation()) {
                                throw new IllegalArgumentException("Ambigous bean publishing", export.exportAction.getSite().getStackTraceAsExcpetion());
                            }
                        }
                    }
                }
            }            
        }

        protected void initCheckpoints() {
            for(CheckpointInfo ch: sourceGraph) {
                CheckpointState cs = new CheckpointState();
                CheckpointState dep = null;
                if (ch.checkpointDependency != null) {
                    for(CheckpointState dcs: checkpoints) {
                        if (dcs.seqNo == ch.checkpointDependency.id) {
                            dep = dcs;
                        }
                    }
                }
                checkpoints.add(cs);
                cs.seqNo = ch.id;
                cs.name = ch.name;
                cs.description = ch.description;
                cs.site = ch.site;
                cs.cpDependency = dep;
                if (ch.scoped) {
                    cs.splitStates = new HashMap<RuntimeEnvironment.ExecutionHost, RuntimeGraph.CheckpointState>();
                }
                
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
            else if (isExportAction(a)) {
                // This is export action
                ExportRef eref = new ExportRef();
                initProtoBean((LocalBean) a.getBeanParams()[0]);
                eref.bean = (LocalBean) a.getBeanParams()[0];
                eref.exportAction = a;
                eref.identity = new BeanIdentity(eref.bean.getType(), (Object[])a.getGroundParams()[1]);
                exports.add(eref);
                ProtoAction pa = new ProtoAction();
                pa.action = a;
                eref.protoAction = pa;
                protoActions.put(a, pa);
                ActionGraph.Bean host = a.getHostBean();
                eref.locators.add(host);
                if (host == omniLocator) {
                    eref.omniLocation = true;
                }
                pa.locators.addAll(eref.locators);
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

        private boolean isExportAction(org.gridkit.lab.gridbeans.ActionGraph.Action a) {
            return a.getSite().allMethodAliases().contains(EXPORT_METHOD);
        }

        protected void initProtoBean(ActionGraph.Bean bean) {
            ActionGraph.LocalBean lbean = (LocalBean) bean;
            if (isInjected(lbean)) {
                initInjectedBean(lbean);
            }
            else {
                initProtoAction(lbean.getOrigin());
            }
        }

        private boolean isInjected(ActionGraph.LocalBean bean) {
            ActionGraph.Action a = bean.getOrigin();
            Bean hostBean = a.getHostBean();
            return (locations.contains(hostBean) || omniLocator == hostBean || rootLocator == hostBean)
                    && a.getSite().allMethodAliases().contains(IMPORT_METHOD);
        }
        
        private void initInjectedBean(ActionGraph.LocalBean bean) {
            if (!injectedBeans.containsKey(bean)) {
                ProtoBean pb = new ProtoBean();
                pb.bean = bean;
                pb.beanIdentity = new BeanIdentity(bean.getOrigin());                
                Bean location = bean.getOrigin().getHostBean();
                pb.locators.add(location);
                if (!locations.contains(location)) {
                    throw new RuntimeException("Unknown location");
                }
                injectedBeans.put(bean, pb);
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

        private void bindExports() {
            // resolve specifics
            for(ExportRef export: exports) {
                if (!export.isOmniLocation()) {
                    bindBean(export);                    
                }
            }       
            // resolve omni locations
            for(ExportRef export: exports) {
                if (export.isOmniLocation()) {
                    matchBean(export);
                }
            }       
        }

        private void matchBean(ExportRef export) {
            for(ProtoBean pb: injectedBeans.values()) {
                if (pb.beanIdentity.equals(export.identity) && pb.binding == null) {
                    pb.binding = export;
                    export.locators.addAll(pb.locators);
                    export.protoAction.locators.addAll(pb.locators);
                }
            }
        }

        private void bindBean(ExportRef export) {
            Set<ExecutionHost> publishLocations = resolveLocation(export.locators);
            
            for(ProtoBean pb: injectedBeans.values()) {
                if (pb.beanIdentity.equals(export.identity)) {
                    Set<ExecutionHost> bt = resolveLocation(pb.locators);
                    if (publishLocations.containsAll(bt)) {
                        if (pb.binding != null) {
                            throw new RuntimeException("Ambigous bean, cannot resolve source", pb.bean.getOrigin().getSite().getStackTraceAsExcpetion());
                        }
                        pb.binding = export;
                    }
                    else {
                        bt.retainAll(publishLocations);
                        if (!bt.isEmpty()) {
                            throw new RuntimeException("Ambigous bean, cannot resolve source", pb.bean.getOrigin().getSite().getStackTraceAsExcpetion());
                        }
                    }
                }
            }
        }

        protected Set<ExecutionHost> resolveLocation(Set<Bean> locators) {
            Set<ExecutionHost> locations;
            locations = new HashSet<RuntimeEnvironment.ExecutionHost>();
            for (ActionGraph.Bean l: locators) {
                if (l != omniLocator) {
                    ExecutionHost[] eh = resolved.get(l);
                    if (eh == null) {
                        throw new RuntimeException("Unresolved bean: " + l);
                    }
                    locations.addAll(Arrays.asList(eh));
                }
            }
            return locations;
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
            boolean exportsApplied = false;
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
                    if (!exportsApplied) {
                        bindExports();
                    }
                    break;
                }
                if (changeCounter == 0) {
                    if (!exportsApplied) {
                        exportsApplied = true;
                        bindExports();
                    }
                    else {
                        throw new RuntimeException("Cannot resolve execution locations");
                    }
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
            if (injectedBeans.containsKey(b)) {
                ProtoBean pb = injectedBeans.get(b);
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
                if (spa.locators.contains(omniLocator)) {
                    if (spa.locators.addAll(pa.locators)) {
                        ++changeCounter;
                    }
                }
            }            
        }
        
        private void propagateLocation(Bean b, ProtoAction pa) {
            if (injectedBeans.containsKey(b)) {
                ProtoBean pb = injectedBeans.get(b);
                if (!pb.locators.containsAll(pa.locators)) {
                    if (!pa.locators.containsAll(pb.locators)) {
                        throw new RuntimeException("Internal error: cannot narrow location");
                    }
                    else {
                        // push up locations from action to bean
                        if (pb.locators.addAll(pa.locators)) {
                            ++changeCounter;
                        }
                    }
                }
                else if (pa.locators.addAll(pb.locators)) {
                    ++changeCounter;
                }
            }
            else {
                if (locations.contains(b)) {
                    // export action special case
                    // skip
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
                boolean exportAction = isExportAction(pa);
                if (!exportAction) {
                    ensureBean(b);
                }
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
                    
                    BeanHolder hostBean = exportAction ? createStub(host) : resolveBean(host, (LocalBean) b);
                    Action a = createAction(host, hostBean, pa.action, groundArgs, beanArgs);
                    Bean outBean = pa.action.getResultBean();
                    if (exportAction) {
                        a.beanDeps.remove(a.hostBean);
                        a.hostBean = null;
                        BeanHolder in = beanArgs[0];
                        BeanHolder oh = createProducedBeanHolder(host, in.beanType, a);
                        a.outputBean = oh;
                        for(ExportRef er: exports) {
                            if (er.protoAction == pa) {
                                ExportTarget et = new ExportTarget(host, er.identity);
                                if (exportMap.containsKey(et)) {
                                    throw new RuntimeException("Ambiguos bean publish " + er.identity + " at " + host, pa.action.getSite().getStackTraceAsExcpetion());
                                }
                                exportMap.put(et, a);
                            }
                        }
                    }
                    else if (outBean != null && !outBean.getGraph().allConsumers(outBean).isEmpty()) {
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

        private BeanHolder createStub(ExecutionHost host) {
            BeanHolder stub = new BeanHolder();
            stub.beanId = -1;
            stub.beanType = ExecutionTarget.class;
            stub.host = host;
            return stub;
        }

        private boolean isExportAction(ProtoAction pa) {
            return locations.contains(pa.action.getHostBean()) && pa.action.getSite().allMethodAliases().contains(EXPORT_METHOD);
        }

        private BeanHolder resolveBean(ExecutionHost host, LocalBean ab) {
            if (injectedBeans.containsKey(ab)) {
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
            ProtoBean pb = injectedBeans.get(b);
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
                    throw new RuntimeException("Runtime scope is not resolve " + pb.beanIdentity, pb.bean.getOrigin().getSite().getStackTraceAsExcpetion());
                }
                for(ExecutionHost host: targets) {
                    if (pb.binding == null) {
                        if (!host.checkBean(pb.beanIdentity.lookupType, pb.beanIdentity.lookupId)) {
                            throw new RuntimeException("Cannot resolve bean " + pb.beanIdentity, pb.bean.getOrigin().getSite().getStackTraceAsExcpetion());
                        }
                    }
                    createExportedBeanHolder(host, pb.beanIdentity);
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
        
        private void linkExports() {
            for(BeanHolder bh: beans) {
                if (bh.lookupId != null) {
                    ExportTarget et = new ExportTarget(bh.host, bh.lookupId);
                    Action pa = exportMap.get(et);
                    if (pa != null) {
                        bh.producerAction = pa;
                    }
                }
            }
        }
        
        private void initDependencies() {
            for(Action a: actions) {
                for(BeanHolder bh: a.beanDeps) {
                    bh.dependents.add(a);
                }
            }
            for(CheckpointState cs: new ArrayList<CheckpointState>(checkpoints)) {
                for(Action a: cs.dependents) {
                    a.checkpointDeps.add(scopedCheckpoint(cs, a.host));
                    if (cs.isScoped()) {
                        scopedCheckpoint(cs, a.host).dependents.add(a);
                    }
                }
                for(Action a: cs.dependencies) {
                    a.dependents.add(scopedCheckpoint(cs, a.host));
                    if (cs.isScoped()) {
                        scopedCheckpoint(cs, a.host).dependencies.add(a);
                    }
                }
            }

            for(CheckpointState cs: new ArrayList<CheckpointState>(checkpoints)) {
                if (cs.cpDependency != null) {
                    cs.cpDependency.cpDependents.add(cs);
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
        BeanIdentity beanIdentity;
        
        ExportRef binding;
    }
    
    private static class ExportRef {
        
        BeanIdentity identity;
        ActionGraph.LocalBean bean;
        ActionGraph.Action exportAction;
        ProtoAction protoAction;
        Set<Bean> locators = new LinkedHashSet<ActionGraph.Bean>();
        boolean omniLocation;
        
        public boolean isOmniLocation() {
            return omniLocation;
        }
        
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
            if (hostBean == null) {
                return method.getName() + "(" + beanParams[0] + ", " + Arrays.toString((Object[])groundParams[1]) + ")";
            }
            else {
                return hostBean + "." + method.getName() + "()";
            }
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
        String description;
        StackTraceElement[] site;
        
        Map<ExecutionHost, CheckpointState> splitStates;
        
        ExecutionHost host;
        
        CheckpointState cpDependency;
        List<Action> dependencies = new ArrayList<Action>();
        List<Action> dependents = new ArrayList<Action>();
        List<CheckpointState> cpDependents = new ArrayList<CheckpointState>();

        boolean passed;
        
        public boolean isScoped() {
            return splitStates != null;
        }
        
        public ExecutionHost getHost() {
            return host;
        }
        
        public String toString() {
            return description;
        }
    }
    
    static class BeanHolder {

        int beanId;
        
        Class<?> beanType;
        RuntimeEnvironment.ExecutionHost host;
        
        // bean can be either
        // produced by action (may be published)
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
        throw new RuntimeException("No such bean found " + bi + " at " + host);
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
    
    
    protected CheckpointState scopedCheckpoint(CheckpointState cs, ExecutionHost host) {
        if (!cs.isScoped()) {
            return cs;
        }
        
        CheckpointState st = cs.splitStates.get(host);
        if (st == null) {
            st = new CheckpointState();
            checkpoints.add(st);
            st.seqNo = cs.seqNo;
            st.name = cs.name;
            st.description = cs.description;
            st.host = host;
            st.site = cs.site;
            cs.splitStates.put(host,  st);
            if (cs.cpDependency != null) {
                CheckpointState dep;
                if (cs.cpDependency.isScoped()) {
                    dep = scopedCheckpoint(cs.cpDependency, host);
                }
                else {
                    dep = cs.cpDependency;
                }
                st.cpDependency = dep;
            }
        }
        return st;
    }
    
    private static class ExportTarget {
        
        private ExecutionHost target;
        private BeanIdentity id;
        
        public ExportTarget(ExecutionHost target, BeanIdentity id) {
            this.target = target;
            this.id = id;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result + ((target == null) ? 0 : target.hashCode());
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
            ExportTarget other = (ExportTarget) obj;
            if (id == null) {
                if (other.id != null)
                    return false;
            } else if (!id.equals(other.id))
                return false;
            if (target == null) {
                if (other.target != null)
                    return false;
            } else if (!target.equals(other.target))
                return false;
            return true;
        }
        
        @Override
        public String toString() {
            return id + "@" + target;
        }
    }
    
    private static class ChDescr implements ExecutionGraph.Checkpoint {
        
        private CheckpointState ch;

        public ChDescr(CheckpointState ch) {
            this.ch = ch;
        }

        @Override
        public String label() {
            return ch.toString();
        }

        @Override
        public Set<Step> dependencies() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<Step> dependents() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TopologyNode host() {
            return ch.host;
        }

        @Override
        public ActionSite callSite() {
            return null;
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
        public void onCheckpoint(ExecutionGraph.Checkpoint checkpoint) {
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
