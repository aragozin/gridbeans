package org.gridkit.lab.gridbeans.monadic;

import java.lang.reflect.Method;
import java.util.Set;

public interface RuntimeEnvironment extends RuntimeTopology {

    public ExecutionHost root();
    
    public ExecutionHost lookupHost(TopologyNode node);
        
    public interface BeanHandle {

        public ExecutionHost getHost();
        
        public void fire(Invocation call, InvocationCallback callback);
        
    }
    
    public interface ExecutionHost extends TopologyNode {

        public Set<ExecutionHost> resolveLocator(Method method, Object[] params);

        public void resolveBean(Class<?> type, Object[] identity, InvocationCallback callback);
    }    
    
    public interface InvocationCallback {
        
        public void done(BeanHandle handle);

        public void error(Exception error);
        
    }
    
    public static class Invocation {
        
        private Method method;
        private Object[] groundParams;
        private BeanHandle[] beanParams;
        private Class<?> outputType;
        
        public Invocation(Method m) {
            this.method = m;
            groundParams = new Object[m.getParameterTypes().length];
            beanParams = new BeanHandle[m.getParameterTypes().length];
        }
        
        public void setGroundParam(int n, Object value) {
            groundParams[n] = value;
        }

        public void setBeanParam(int n, BeanHandle handle) {
            beanParams[n] = handle;
        }
        
        public void setOutputType(Class<?> type) {
            if (!type.isInterface()) {
                throw new IllegalArgumentException("Should be an interface - " + type.getName());
            }
            outputType = type;
        }
        
        public Method getMethod() {
            return method;
        }
        
        public Object getGroundParam(int n) {
            return groundParams[n];
        }

        public BeanHandle getBeanParam(int n) {
            return beanParams[n];
        }
        
        public Class<?> getOutputType() {
            return outputType;
        }
    }
}
