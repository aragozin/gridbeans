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

import org.gridkit.lab.gridbeans.ActionGraph;
import org.gridkit.lab.gridbeans.ActionGraph.Bean;
import org.gridkit.lab.gridbeans.ActionGraph.LocalBean;
import org.gridkit.lab.gridbeans.monadic.Checkpoint;
import org.gridkit.lab.gridbeans.monadic.ExecutionTarget;
import org.gridkit.lab.gridbeans.monadic.Monad.ExecutionObserver;
import org.gridkit.lab.gridbeans.monadic.builder.RawGraphData.CheckpointInfo;
import org.gridkit.lab.gridbeans.monadic.spi.MonadExecutionEnvironment;
import org.gridkit.lab.gridbeans.monadic.spi.MonadExecutionEnvironment.ExecutionHost;

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

    
    private MonadExecutionEnvironment environment;
    private List<CheckpointState> checkpoints = new ArrayList<CheckpointState>();
    private List<Action> actions = new ArrayList<Action>();
    private List<BeanHolder> beans = new ArrayList<BeanHolder>();
    
    public RuntimeGraph(RawGraphData graph, MonadExecutionEnvironment environment) {
        this.environment = environment;
        new GraphProcessor(graph).process();
    }

    public void run(ExecutionObserver observer) {
        // TODO Auto-generated method stub
        
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
                
                Set<ExecutionHost> th = new LinkedHashSet<MonadExecutionEnvironment.ExecutionHost>();
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
//                for(ProtoBean pb: exportedBeans.values()) {
//                    int n = pb.locators.size();
//                    tryLocalize(pb);
//                    if (pb.locators.isEmpty()) {
//                        ++nUnresolved;
//                    }
//                    else if (n != pb.locators.size()) {
//                        ++nResolved;
//                        
//                    }
//                }
                
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
            Set<ExecutionHost> targets = new LinkedHashSet<MonadExecutionEnvironment.ExecutionHost>();
            for(Bean locator: locators) {
                if (locator != omniLocator) {
                    for(ExecutionHost host: resolved.get(locator)) {
                        targets.add(host);
                    }
                }
            }
            
            return targets;
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
        MonadExecutionEnvironment.ExecutionHost host;
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
        MonadExecutionEnvironment.ExecutionHost host;
        
        // bean can be either
        // produced by action
        Action producerAction;
        // or injected by environment
        BeanIdentity lookupId;
        
        // state
        boolean requested;
        MonadExecutionEnvironment.BeanHandle handle;
        
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
}
