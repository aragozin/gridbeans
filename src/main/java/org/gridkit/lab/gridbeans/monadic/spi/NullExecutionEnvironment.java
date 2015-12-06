package org.gridkit.lab.gridbeans.monadic.spi;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.gridkit.lab.gridbeans.PowerBeanProxy;
import org.gridkit.lab.gridbeans.PowerBeanProxy.InvocationProcessor;
import org.gridkit.lab.gridbeans.monadic.Locator;

public class NullExecutionEnvironment implements MonadExecutionEnvironment {

    private static final Method CREATE_HOST_METHOD;

    static {
        try {
            CREATE_HOST_METHOD = NullExecutionHost.class.getMethod("createHost", Class.class);
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
    
    
    NullExecutionHost root;
    
    public NullExecutionEnvironment() {
        root = new HostStub("root");
    }
    
    
    @Override
    public NullExecutionHost root() {
        return root;
    }
    
    public interface NullExecutionHost extends ExecutionHost {
        
        public <T extends Locator> T createHost(Class<T> locatorType);
        
    }
    
    private class HostStub implements NullExecutionHost, InvocationProcessor {

        String name;
        Map<Location, ExecutionHost> locations = new HashMap<NullExecutionEnvironment.Location, MonadExecutionEnvironment.ExecutionHost>();
        NullExecutionHost nullDelegate;
        
        public HostStub(String name) {
            this.name = name;
            this.nullDelegate = PowerBeanProxy.powerProxy(this, NullExecutionHost.class);
        }

        @Override
        public void process(PowerBeanProxy.Invocation invocation) {
            if (invocation.getAlternativeMethods().contains(CREATE_HOST_METHOD)) {
                final Class<?> locator = (Class<?>) invocation.getArguments()[0];
                invocation.doReturnProxy(new InvocationProcessor() {
                    @Override
                    public void process(PowerBeanProxy.Invocation invocation) {
                        createLocation(invocation.getCastedMethod(locator), invocation.getArguments());
                        invocation.doReturnObject(null);
                    }
                });
            }            
        }

        protected void createLocation(Method m, Object[] params) {
            Location loc = new Location(m.getDeclaringClass(), m, params);
            if (!locations.containsKey(loc)) {
                HostStub stub = new HostStub(m.getDeclaringClass().getSimpleName() + "." + m.getName() + Arrays.toString(params));
                locations.put(loc, stub);
            }
        }
        
        @Override
        public Set<ExecutionHost> resolveLocator(Method method, Object[] params) {
            Location l = new Location(method.getDeclaringClass(), method, params);
            if (locations.containsKey(l)) {
                return Collections.singleton(locations.get(l));
            }
            else {
                return Collections.emptySet();
            }
        }

        @Override
        public void resolveBean(Class<?> type, Object[] identity, InvocationCallback callback) {
            callback.done(new VoidBeanHandle());
        }
        
        @Override
        public <T extends Locator> T createHost(Class<T> locatorType) {
            return nullDelegate.createHost(locatorType);
        }

        public String toString() {
            return name;
        }
    }
    
    private class VoidBeanHandle implements BeanHandle {

        ExecutionHost host;
        
        @Override
        public ExecutionHost getHost() {
            return host;
        }

        @Override
        public void fire(Invocation call, InvocationCallback callback) {
            callback.done(new VoidBeanHandle());
        }
    }
    
    private static class Location {
        
        private Class<?> locator;
        private Method method;
        private Object[] params;
        
        public Location(Class<?> locator, Method method, Object[] params) {
            this.locator = locator;
            this.method = method;
            this.params = params;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((locator == null) ? 0 : locator.hashCode());
            result = prime * result + ((method == null) ? 0 : method.hashCode());
            result = prime * result + Arrays.hashCode(params);
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
            Location other = (Location) obj;
            if (locator == null) {
                if (other.locator != null)
                    return false;
            } else if (!locator.equals(other.locator))
                return false;
            if (method == null) {
                if (other.method != null)
                    return false;
            } else if (!method.equals(other.method))
                return false;
            if (!Arrays.equals(params, other.params))
                return false;
            return true;
        }
    }
}
