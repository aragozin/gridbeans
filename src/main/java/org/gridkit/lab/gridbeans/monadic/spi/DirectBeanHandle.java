package org.gridkit.lab.gridbeans.monadic.spi;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;

import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.BeanHandle;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.ExecutionHost;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.Invocation;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.InvocationCallback;

public class DirectBeanHandle implements BeanHandle {

    private final ExecutionHost host;
    private final Object bean;
    
    public DirectBeanHandle(ExecutionHost host, Object bean) {
        this.host = host;
        this.bean = bean;
    }

    @Override
    public ExecutionHost getHost() {
        return host;
    }
    
    Object getBean() {
        return bean;
    }

    @Override
    public void fire(Invocation call, InvocationCallback callback) {
        Method m = call.getMethod();
        Object[] args = new Object[m.getParameterTypes().length];
        
        for(int i = 0; i != args.length; ++i) {
            BeanHandle bh = call.getBeanParam(i);
            if (bh != null) {
                if (!(bh instanceof DirectBeanHandle)) {
                    callback.error(new ExecutionException(new RuntimeException("Cannot reference bean: " + bh)));
                    return;
                }
                args[i] = ((DirectBeanHandle)bh).getBean();
            }
            else {
                args[i] = call.getGroundParam(i);
            }
        }

        try {
            m.setAccessible(true);
            Object r = m.invoke(bean, args);
            if (call.getOutputType() != null) {
                callback.done(new DirectBeanHandle(getHost(), r));
            }
            else {
                callback.done(null);
            }
        } catch (InvocationTargetException e) {
            if (e.getTargetException() instanceof Exception) {
                callback.error((Exception) e.getTargetException());
            }
            else {
                callback.error(e);
            }
        } catch (Exception e) {
            callback.error(new ExecutionException(e));
        }        
    }
}
