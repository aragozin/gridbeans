package org.gridkit.lab.gridbeans.monadic.spi;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gridkit.lab.gridbeans.PowerBeanProxy;
import org.gridkit.lab.gridbeans.PowerBeanProxy.InvocationProcessor;
import org.gridkit.lab.gridbeans.monadic.Locator;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment;

public class GenericEnvironment implements RuntimeEnvironment {

    private HostEntity root = new HostEntity("{root}");
    private BeanFactoryBuilder globalBeans = new BeanFactoryBuilder();
    
    @Override
    public ExecutionHost root() {
        return root.getHost();
    }

    @Override
    public ExecutionHost lookupHost(TopologyNode node) {
        return ((ExecutionHost)node);
    }



    public HostBuilder rootBuilder() {
        return root;
    }
    
    public BeanFactoryBuilder globalBeanBuilder() {
        return globalBeans;
    }
    
    public HostBuilder createHost(String displayName) {
        return new HostEntity(displayName);
    }
    
    public interface HostBuilder {
        
        public ExecutionHost getHost();
        
        public void injectBean(BeanProducer bprod, Class<?> type, Object... id);

        public void injectBean(BeanHandle handle, Class<?> type, Object... id);

        public void injectFactory(BeanFactory factory);
        
        public <T extends Locator> T injectLocation(ExecutionHost host, Class<T> ltype);

        public void injectLocator(LocationResolver locator, Class<? extends Locator> ltype);
        
    }
    
    public interface BeanFactory {

        /**
         * Bean may be produced asynchronously, but
         * factory should indicate if it is responsible for this bean
         * or not.
         * <p>
         * <code>true</code> - result means bean will be produced by factory
         * <br/>
         * <code>false</code> - result means that factory will not handle this bean
         */
        public boolean lookup(ExecutionHost host, Class<?> type, Object[] id, InvocationCallback callback); 
        
        /**
         * Verify bean presence.
         * <p>
         * <code>true</code> - result means bean will be produced by factory
         * <br/>
         * <code>false</code> - result means that factory will not handle this bean
         */
        public boolean checkBean(Class<?> type, Object[] id);
    }    

    public interface LocationResolver {
        
        public Set<ExecutionHost> locate(Method method, Object[] params); 
        
    }    

    public interface BeanProducer {
        
        public void produce(ExecutionHost host, InvocationCallback callback); 
        
    }   
    
    private class HostEntity implements HostBuilder {
        
        private String displayName;
        private ExecHost host = new ExecHost();
        private BeanFactoryBuilder factoryBuilder = new BeanFactoryBuilder() {

            @Override
            protected List<BeanFactory> getDelegates() {
                List<BeanFactory> d = new ArrayList<GenericEnvironment.BeanFactory>(super.getDelegates());
                d.add(globalBeans);
                return d;
            }
            
        };
        private LocationBuilder locationBuilder = new LocationBuilder();
        
        public HostEntity(String displayName) {
            this.displayName = displayName;
        }

        @Override
        public ExecutionHost getHost() {
            return host;
        }

        @Override
        public void injectBean(BeanProducer bprod, Class<?> type, Object... id) {
            factoryBuilder.injectBean(bprod, type, id);                        
        }

        @Override
        public void injectBean(BeanHandle handle, Class<?> type, Object... id) {
            factoryBuilder.injectBean(handle, type, id);                        
        }

        @Override
        public void injectFactory(BeanFactory factory) {
            factoryBuilder.injectFactory(factory);                        
        }

        @Override
        public <T extends Locator> T injectLocation(ExecutionHost host, Class<T> ltype) {
            return locationBuilder.injectLocation(host, ltype);
        }

        @Override
        public void injectLocator(LocationResolver locator, Class<? extends Locator> ltype) {
            locationBuilder.injectLocator(locator, ltype);
        }

        @Override
        public String toString() {
            return displayName;
        }
        
        private class ExecHost implements ExecutionHost {

            @Override
            public Set<ExecutionHost> resolveLocator(Method method, Object[] params) {                
                return locationBuilder.locate(method, params);
            }

            @Override
            public boolean checkBean(Class<?> type, Object[] identity) {
                return factoryBuilder.checkBean(type, identity);                
            }

            @Override
            public void resolveBean(Class<?> type, Object[] identity, InvocationCallback callback) {
                factoryBuilder.lookup(this, type, identity, callback);                
            }
            
            @Override
            public String toString() {
                return displayName;
            }
        }
    }

    private static class MappedBeanFactory implements BeanFactory {

        private Map<BeanIdentity, BeanProducer> beans = new HashMap<GenericEnvironment.BeanIdentity, GenericEnvironment.BeanProducer>();

        @Override
        public boolean lookup(ExecutionHost host, Class<?> type, Object[] id, InvocationCallback callback) {
            BeanIdentity bi = new BeanIdentity(type, id);
            BeanProducer p = beans.get(bi);
            if (p != null) {
                p.produce(host, callback);
                return true;
            }
            else {
                return false;
            }
        }

        
        @Override
        public boolean checkBean(Class<?> type, Object[] id) {
            BeanIdentity bi = new BeanIdentity(type, id);
            BeanProducer p = beans.get(bi);
            if (p != null) {
                return true;
            }
            else {
                return false;
            }
        }

        public void put(Class<?> type, Object[] id, BeanProducer producer) {
            BeanIdentity bi = new BeanIdentity(type, id);
            beans.put(bi, producer);
        }
        
    }
    
    private static class DirectBeanProducer implements BeanProducer {

        private final BeanHandle handle;
        
        public DirectBeanProducer(BeanHandle handle) {
            this.handle = handle;
        }

        @Override
        public void produce(ExecutionHost host, InvocationCallback callback) {
            if (handle.getHost() != host) {
                throw new IllegalArgumentException("Handle belongs to wrong host");
            }
            callback.done(handle);
        }
    }
    
    private static class BeanFactoryBuilder implements BeanFactory, HostBuilder {
        
        private List<BeanFactory> delegates = new ArrayList<BeanFactory>();
        private MappedBeanFactory mappedBeans = new MappedBeanFactory();
        {
            delegates.add(mappedBeans);
        }

        @Override
        public ExecutionHost getHost() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean lookup(ExecutionHost host, Class<?> type, Object[] id, InvocationCallback callback) {
            for(BeanFactory f: getDelegates()) {
                if (f.lookup(host, type, id, callback)) {
                    return true;
                }
            }
            callback.error(new IllegalArgumentException("No such bean found: " + type.getName() + (id.length == 0 ? "" : Arrays.toString(id))));
            return true;
        }

        @Override
        public boolean checkBean(Class<?> type, Object[] id) {
            for(BeanFactory f: getDelegates()) {
                if (f.checkBean(type, id)) {
                    return true;
                }
            }
            return false;
        }

        protected List<BeanFactory> getDelegates() {
            return delegates;
        }
        
        @Override
        public void injectBean(BeanProducer bprod, Class<?> type, Object... id) {
            mappedBeans.put(type, id, bprod);            
        }

        @Override
        public void injectBean(BeanHandle handle, Class<?> type, Object... id) {
            mappedBeans.put(type, id, new DirectBeanProducer(handle));            
        }

        @Override
        public void injectFactory(BeanFactory factory) {
            getDelegates().add(factory);            
        }

        @Override
        public <T extends Locator> T injectLocation(ExecutionHost host, Class<T> ltype) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void injectLocator(LocationResolver locator, Class<? extends Locator> ltype) {
            throw new UnsupportedOperationException();
        }
    }
    
    private static class LocationBuilder implements HostBuilder, LocationResolver {
        
        Map<LocationIdentity, Set<ExecutionHost>> mappedLocations = new HashMap<GenericEnvironment.LocationIdentity, Set<ExecutionHost>>();
        Map<Class<?>, LocationResolver> locators = new HashMap<Class<?>, GenericEnvironment.LocationResolver>();

        @Override
        public Set<ExecutionHost> locate(Method method, Object[] params) {
            LocationIdentity li = new LocationIdentity(method, params);
            Set<ExecutionHost> set = mappedLocations.get(li);
            if (set != null) {
                return new HashSet<ExecutionHost>(set);
            }
            else {
                Class<?> c = method.getDeclaringClass();
                LocationResolver l = locators.get(c);
                if (l != null) {
                    return l.locate(method, params);
                }
            }
            
            throw new IllegalArgumentException("Unknown location: " + method);
        }
        
        @Override
        public ExecutionHost getHost() {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public void injectBean(BeanProducer bprod, Class<?> type, Object... id) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void injectBean(BeanHandle handle, Class<?> type, Object... id) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void injectFactory(BeanFactory factory) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T extends Locator> T injectLocation(final ExecutionHost host, final Class<T> ltype) {
            if (!ltype.isInterface()) {
                throw new IllegalArgumentException("Interface required: " + ltype);
            }
            
            InvocationProcessor ip = new InvocationProcessor() {
                
                @Override
                public void process(PowerBeanProxy.Invocation invocation) {
                    Method m = invocation.tryCastMethod(ltype);
                    if (m != null) {
                        LocationIdentity li = new LocationIdentity(m, invocation.getArguments());
                        Set<ExecutionHost> set = mappedLocations.get(li);
                        if (set == null) {
                            set = new HashSet<RuntimeEnvironment.ExecutionHost>();
                            mappedLocations.put(li, set);
                        }
                        set.add(host);
                        invocation.doReturnObject(null);
                    }
                }
            };
            
            return PowerBeanProxy.powerProxy(ip, ltype);
        }
        
        @Override
        public void injectLocator(LocationResolver locator, Class<? extends Locator> ltype) {
            locators.put(ltype, locator);            
        }
    }
    
    static class BeanIdentity {
        
        Class<?> lookupType;
        Object[] lookupId;
        
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
    
    static class LocationIdentity {
        
        Method lookupMethod;
        Object[] lookupId;
        
        public LocationIdentity(Method lookupMethod, Object[] lookupId) {
            this.lookupMethod = lookupMethod;
            this.lookupId = lookupId;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Arrays.hashCode(lookupId);
            result = prime * result + ((lookupMethod == null) ? 0 : lookupMethod.hashCode());
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
            LocationIdentity other = (LocationIdentity) obj;
            if (!Arrays.equals(lookupId, other.lookupId))
                return false;
            if (lookupMethod == null) {
                if (other.lookupMethod != null)
                    return false;
            } else if (!lookupMethod.equals(other.lookupMethod))
                return false;
            return true;
        }
        
        public String toString() {
            return lookupMethod.getDeclaringClass().getSimpleName() + lookupMethod.getName() + Arrays.toString(lookupId);
        }
    }    
}
