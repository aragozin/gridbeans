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
	private transient Object remoteResolved;

	public RemoteBeanHandler(AsyncBeanHandler target) {
		this.handler = new RemotePart(target);
	}

	protected RemoteBeanHandler(RemoteHandler handler) {
		this.handler = handler;
	}

	@Override
	public Class<?> getType() {
		return handler.getType();
	}

	@Override
	public Object resolve() {
		return handler.resolve();
	}

	@Override
	public FutureEx<AsyncBeanHandler> fire(Method m, Object[] args) {
		// TODO lookup matching method in target class
		FutureBox<AsyncBeanHandler> box = new FutureBox<AsyncBeanHandler>();
		try {
			box.setData(new RemoteBeanHandler(handler.fire(m, args)));
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

		public RemoteHandler fire(Method m, Object[] args) throws Throwable;
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
		public RemoteHandler fire(Method m, Object[] args) throws Throwable {
			FutureEx<AsyncBeanHandler> f = target.fire(m, args);
			try {
				return new RemotePart(f.get());
			}
			catch(ExecutionException e) {
				throw e.getCause();
			}			
		}
	}
	
}
