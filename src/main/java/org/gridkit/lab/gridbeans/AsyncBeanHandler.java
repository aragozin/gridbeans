package org.gridkit.lab.gridbeans;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import org.gridkit.util.concurrent.FutureBox;
import org.gridkit.util.concurrent.FutureEx;

public interface AsyncBeanHandler {

	public Class<?> getType();
	
	public Object resolve();

	public FutureEx<AsyncBeanHandler> fire(Method m, Object[] args);

	public static class DirectHandler implements AsyncBeanHandler, Serializable {
		
		private static final long serialVersionUID = 20131208L;
		
		private Object target;

		public DirectHandler(Object target) {
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
	}
	
	public static class OffThreadAdapter implements AsyncBeanHandler {

		private AsyncBeanHandler target;
		private Executor executor;

		public OffThreadAdapter(AsyncBeanHandler target, Executor exec) {
			this.target = target;
			this.executor = exec;
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
		public FutureEx<AsyncBeanHandler> fire(final Method m, final Object[] args) {
			// TODO lookup
			final FutureBox<AsyncBeanHandler> box = new FutureBox<AsyncBeanHandler>();
			executor.execute(new Runnable() {
				@Override
				public void run() {
					String tn = Thread.currentThread().getName();
					Thread.currentThread().setName(tn + "CALLING: " + m.getDeclaringClass().getSimpleName() + "#" + m.getName());
					FutureEx<AsyncBeanHandler> future = target.fire(m, args);
					try {
						
						box.setData(new OffThreadAdapter(future.get(), executor));
					}
					catch(ExecutionException e) {
						box.setError(e.getCause());
					} catch (InterruptedException e) {
						box.setError(e);
					}					
					finally {
						Thread.currentThread().setName(tn);
					}
				}
			});
			return box;
		}
	}		
}
