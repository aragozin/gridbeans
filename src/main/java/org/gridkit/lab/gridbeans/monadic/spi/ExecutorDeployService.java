package org.gridkit.lab.gridbeans.monadic.spi;

import java.util.concurrent.Executor;

import org.gridkit.lab.gridbeans.monadic.DeployerSPI;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.BeanHandle;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.ExecutionHost;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.Invocation;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.InvocationCallback;

/**
 * This is implementation of handler for {@link DeployerSPI} service. 
 * This implementation is deploying
 * bean to dedicated executor (delegating bean operations).
 * <p>
 * Suitable as example and test utility.
 * 
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 *
 */
public class ExecutorDeployService implements BeanHandle {

    private ExecutionHost host;
    private Executor exec;
    
    public ExecutorDeployService(ExecutionHost host, Executor exec) {
        this.host = host;
        this.exec = exec;
    }

    @Override
    public ExecutionHost getHost() {
        return host;
    }

    public BeanHandle wrap(BeanHandle handler) {
        return new BeanWrapper(handler);
    }
    
    @Override
    public void fire(Invocation call, InvocationCallback callback) {
        if ("deploy".equals(call.getMethod().getName())) {
            deploy(call, callback);
        }
        else {
            callback.error(new RuntimeException("Unknown method: " + call.getMethod()));
        }        
    }
    
    private void deploy(Invocation call, InvocationCallback callback) {
        Class<?> type = (Class<?>) call.getGroundParam(0);
        Object proto = call.getGroundParam(1);
        proto = type.cast(proto);
        DirectBeanHandle dbh = new DirectBeanHandle(host, proto);
        callback.done(new BeanWrapper(dbh));        
    }

    private class BeanWrapper extends WrapperBeanHandler {

        public BeanWrapper(BeanHandle delegate) {
            super(delegate);
        }

        @Override
        public ExecutionHost getHost() {
            return ExecutorDeployService.this.getHost();
        }

        @Override
        protected void delegate(final Invocation call, final InvocationCallback callback) {
            exec.execute(new Runnable() {
                @Override
                public void run() {
                    getDelegate().fire(call, callback);                    
                }
            });
        }

        @Override
        protected BeanHandle wrap(BeanHandle handle) {
            return new BeanWrapper(handle);
        }
    }
}
