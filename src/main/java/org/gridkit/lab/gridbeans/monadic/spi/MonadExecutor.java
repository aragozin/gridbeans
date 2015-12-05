package org.gridkit.lab.gridbeans.monadic.spi;

import java.lang.reflect.Method;
import java.util.List;

import org.gridkit.util.concurrent.FutureEx;

public interface MonadExecutor {

    public ExecutionHost root();
    
    public interface BeanHandle {

        public ExecutionHost getHost();
        
        public FutureEx<Void> fireVoid(Method m, Object... params);

        public FutureEx<BeanHandle> fireAndReturn(Method m, Object... params);
        
    }
    
    public interface LocatorHander {
        
        List<ExecutionHost> resolveLocation(String methodName, Object... params);
        
    }
    
    public interface ExecutionHost {

        public LocatorHander resolveLocator(Class<?> type, Object identity);

        public BeanHandle resolveBean(Class<?> type, Object identity);
        
        public BeanHandle deploy(Object object);        
    }
}
