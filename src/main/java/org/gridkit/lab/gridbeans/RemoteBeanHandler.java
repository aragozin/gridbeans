package org.gridkit.lab.gridbeans;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.rmi.Remote;
import java.util.concurrent.ExecutionException;

import org.gridkit.util.concurrent.FutureBox;
import org.gridkit.util.concurrent.FutureEx;

public class RemoteBeanHandler implements AsyncBeanHandler, Serializable {

	private static final long serialVersionUID = 20131221L;
	
	private RemoteHandler handler;
	
	private transient Class<?> remoteType;
	private transient boolean isResolved = false;
	private transient Object remoteResolved;

	public RemoteBeanHandler(AsyncBeanHandler target) {
		this.handler = new RemotePart(target);
	}

	protected RemoteBeanHandler(RemoteHandler handler) {
		this.handler = handler;
	}

	@Override
	public synchronized Class<?> getType() {
		if (remoteType == null) {
			remoteType = handler.getType(); 
		}
		return remoteType;
	}

	@Override
	public Object resolve() {
		if (!isResolved) {
			remoteResolved = handler.resolve();
			isResolved = true;			 
		}
		return remoteResolved;
	}

	@Override
	public FutureEx<AsyncBeanHandler> fire(Method m, Object[] args) {
		FutureBox<AsyncBeanHandler> box = new FutureBox<AsyncBeanHandler>();
		try {
			box.setData(new RemoteBeanHandler(handler.fire(new MethodRef(m), args)));
		} catch (ThreadDeath e) {
			throw e;
		} catch (Throwable e) {
			box.setError(e);
		}
		return box;
	}
	
	private interface RemoteHandler extends Remote {
		
		public Class<?> getType();


		public Object resolve();

		public RemoteHandler fire(MethodRef m, Object[] args) throws Throwable;
	}
	
	private class RemotePart implements RemoteHandler {

		private AsyncBeanHandler target;
		
		public RemotePart(AsyncBeanHandler target) {
			this.target = target;
		}
		
		@Override
		public Class<?> getType() {
			return target.getType();
		}

		@Override
		public Object resolve() {
			return target.resolve();
		}

		@Override
		public RemoteHandler fire(MethodRef m, Object[] args) throws Throwable {
			FutureEx<AsyncBeanHandler> f = target.fire(m.newMethod(), args);
			try {
				return new RemotePart(f.get());
			}
			catch(ExecutionException e) {
				throw e.getCause();
			}			
		}
	}
	
	static class MethodRef implements Serializable {
	    
		private static final long serialVersionUID = 20140201L;
	    
	    private final Class<?> clazz;
	    private final String name;
	    private final Class<?>[] parameterTypes;
	    
	    public MethodRef(Method method) {
	        clazz = method.getDeclaringClass();
	        name = method.getName();
	        parameterTypes = method.getParameterTypes();
	    }
	    
	    public Method newMethod() throws Exception {
	        return clazz.getDeclaredMethod(name, parameterTypes);
	    }
	    
	    public Object invoke(Object obj, Object... args) throws Exception {
	        return newMethod().invoke(obj, args);
	    }
	}	
}
