package org.gridkit.lab.gridbeans;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.rmi.Remote;
import java.util.ArrayList;
import java.util.List;

import org.gridkit.util.concurrent.FutureBox;
import org.gridkit.util.concurrent.FutureEx;

public class RemoteBeanHandler implements AsyncBeanHandler, Serializable {

	private static final long serialVersionUID = 20131221L;
	
	private Object target;

	public RemoteBeanHandler(Object target) {
		this.target = target;
	}

	@Override
	public Class<?> getType() {
		return target.getClass();
	}

	@Override
	public Object resolve() {
		return target;
	}

	@Override
	public FutureEx<AsyncBeanHandler> fire(Method m, Object[] args) {
		// TODO lookup matching method in target class
		FutureBox<AsyncBeanHandler> box = new FutureBox<AsyncBeanHandler>();
		try {
			box.setData(new DirectHandler(m.invoke(target, args)));
		}
		catch(InvocationTargetException e) {
			box.setError(e.getCause());
		} catch (IllegalArgumentException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
		return box;
	}
	
	private interface RemoteHandler extends Remote {
		
		public Class<?> getType(int id);

		public Object resolve(int id);

		public int fire(int id, Method m, Object[] args);
	}
	
	private class RemotePart implements RemoteHandler {

		public List<Object> targets = new ArrayList<Object>();
		
		
		
		@Override
		public Class<?> getType(int id) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Object resolve(int id) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public int fire(int id, Method m, Object[] args) {
			// TODO Auto-generated method stub
			return 0;
		}

		
	}
	
}
