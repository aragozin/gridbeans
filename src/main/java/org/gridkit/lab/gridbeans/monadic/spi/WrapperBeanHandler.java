package org.gridkit.lab.gridbeans.monadic.spi;

import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.BeanHandle;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.ExecutionHost;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.Invocation;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.InvocationCallback;

public abstract class WrapperBeanHandler implements BeanHandle {

    private final BeanHandle delegate;
    
    public WrapperBeanHandler(BeanHandle delegate) {
        this.delegate = delegate;
    }

    @Override
    public ExecutionHost getHost() {
        return delegate.getHost();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((delegate == null) ? 0 : delegate.hashCode());
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
        WrapperBeanHandler other = (WrapperBeanHandler) obj;
        if (delegate == null) {
            if (other.delegate != null)
                return false;
        } else if (!delegate.equals(other.delegate))
            return false;
        return true;
    }

    @Override
    public void fire(Invocation call, InvocationCallback callback) {
        int paramCount = call.getMethod().getParameterTypes().length;
        for(int i = 0; i != paramCount; ++i) {
            BeanHandle bh = call.getBeanParam(i);
            if (bh != null) {
                bh = unwrap(bh);
                call.setBeanParam(i, bh);
            }
        }        
        delegate(call, new CallbackWrapper(callback));
    }

    protected void delegate(Invocation call, InvocationCallback callback) {
        delegate.fire(call, callback);
    }
    
    public BeanHandle getDelegate() {
        return delegate;
    }
    
    protected BeanHandle unwrap(BeanHandle handle) {
        if (handle.getClass() == getClass()) {
            return ((WrapperBeanHandler)handle).getDelegate();
        }
        else {
            return handle;
        }
    }
    
    protected abstract BeanHandle wrap(BeanHandle handle);
    
    public String toString() {
        return delegate.toString();
    }
    
    private class CallbackWrapper implements InvocationCallback {

        private final InvocationCallback delegate;
        
        public CallbackWrapper(InvocationCallback delegate) {
            this.delegate = delegate;
        }

        @Override
        public void done(BeanHandle handle) {
            if (handle != null) {
                handle = wrap(handle);
            }
            delegate.done(handle);            
        }

        @Override
        public void error(Exception error) {
            delegate.error(error);            
        }
    }
}
