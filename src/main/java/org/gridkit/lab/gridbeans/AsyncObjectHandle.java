package org.gridkit.lab.gridbeans;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Executor;

import org.gridkit.util.concurrent.FutureBox;
import org.gridkit.util.concurrent.FutureEx;

public interface AsyncObjectHandle {

	public Class<?> getType();
	
	public Object resolve();

	public FutureEx<AsyncObjectHandle> fire(Method m, Object[] args);

	public static class DirectHandle implements AsyncObjectHandle, Serializable {
		
		private static final long serialVersionUID = 20131208L;
		
		private Object target;

		public DirectHandle(Object target) {
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
		public FutureEx<AsyncObjectHandle> fire(Method m, Object[] args) {
			// TODO lookup matching method in target class
			FutureBox<AsyncObjectHandle> box = new FutureBox<AsyncObjectHandle>();
			try {
				box.setData(new DirectHandle(m.invoke(target, args)));
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
	}
	
	public static class ExecutorHandle implements AsyncObjectHandle, Serializable {
		
		private static final long serialVersionUID = 20131208L;
		
		private Object target;
		private Executor executor;

		public ExecutorHandle(Object target, Executor exec) {
			this.target = target;
			this.executor = exec;
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
		public FutureEx<AsyncObjectHandle> fire(final Method m, final Object[] args) {
			// TODO lookup
			final FutureBox<AsyncObjectHandle> box = new FutureBox<AsyncObjectHandle>();
			executor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						box.setData(new ExecutorHandle(m.invoke(target, args), executor));
					}
					catch(InvocationTargetException e) {
						box.setError(e.getCause());
					} catch (IllegalArgumentException e) {
						throw new RuntimeException(e);
					} catch (IllegalAccessException e) {
						throw new RuntimeException(e);
					}
				}
			});
			return box;
		}
	}		
}
