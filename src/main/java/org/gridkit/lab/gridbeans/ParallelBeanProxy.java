package org.gridkit.lab.gridbeans;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.gridkit.lab.gridbeans.PowerBeanProxy.Invocation;
import org.gridkit.lab.gridbeans.PowerBeanProxy.InvocationProcessor;
import org.gridkit.util.concurrent.FutureEx;

public class ParallelBeanProxy {

	public static <T> T lump(T... proxies) {
		return lump(Arrays.asList(proxies));
	}

	public static <T> T lump(Collection<T> proxies) {
		T head = proxies.iterator().next();
		List<AsyncObjectHandle> beans = new ArrayList<AsyncObjectHandle>();
		for(T p: proxies) {
			InvocationProcessor ip = PowerBeanProxy.getHandler(p);
			if (ip == null || !(ip instanceof CallProcessor)) {
				throw new IllegalArgumentException("Not a parallel proxy: " + p);
			}
			beans.addAll(((CallProcessor)ip).handles);
		}
		return PowerBeanProxy.cloneProxy(head, new CallProcessor(beans));
	}
	
	public static <T> T proxy(Class<T> type, AsyncObjectHandle... beans) {
		CallProcessor cp = new CallProcessor(Arrays.asList(beans));
		return PowerBeanProxy.powerProxy(cp, type);
	}

	public static <T> T proxy(Class<T> type, Collection<AsyncObjectHandle> beans) {
		CallProcessor cp = new CallProcessor(new ArrayList<AsyncObjectHandle>(beans));
		return PowerBeanProxy.powerProxy(cp, type);		
	}
	
	public static <T> T directProxy(Class<T> type, T... beans) {
		return directProxy(type, Arrays.asList(beans));
	}

	public static <T> T directProxy(Class<T> type, Collection<T> beans) {
		List<AsyncObjectHandle> handles = new ArrayList<AsyncObjectHandle>(beans.size());
		for(T b : beans) {
			handles.add(new AsyncObjectHandle.DirectHandle(b));
		}
		return proxy(type, handles);
	}
	
	private static class CallProcessor implements InvocationProcessor {

		private List<AsyncObjectHandle> handles = new ArrayList<AsyncObjectHandle>();

		public CallProcessor(List<AsyncObjectHandle> handles) {
			this.handles = handles;
		}

		@Override
		public void process(Invocation invocation) {
			List<FutureEx<AsyncObjectHandle>> futures = new ArrayList<FutureEx<AsyncObjectHandle>>(handles.size());
			for(AsyncObjectHandle h: handles) {
				futures.add(h.fire(invocation.getMethod(), invocation.getArguments()));
			}
			List<AsyncObjectHandle> beans = new ArrayList<AsyncObjectHandle>(handles.size());
			try {
				for(FutureEx<AsyncObjectHandle> f: futures) {
					beans.add(f.get());
				}
			} catch (InterruptedException e) {
				invocation.doThrow(new RuntimeException(e));
			} catch (ExecutionException e) {
				invocation.doThrow(e.getCause());
			}
			if (invocation.getReturnType() == void.class) {
				invocation.doReturnObject(null);
			}
			else if (shouldReduce(invocation.getReturnType())) {
				invocation.doReturnObject(reduce(beans));
			}
			else {
				invocation.doReturnProxy(new CallProcessor(beans));
			}
		}

		private boolean shouldReduce(Class<?> returnType) {
			// TODO smart reduction rules
			if (returnType.isInterface()) {
				return false;
			}
			else {
				return true;
			}
		}

		private Object reduce(List<AsyncObjectHandle> beans) {
			if (beans.size() == 1) {
				return beans.get(0).resolve();
			}
			else {
				Object val = beans.get(0).resolve();
				for(int i = 1; i != beans.size(); ++i) {
					Object vn = beans.get(1).resolve();
					if (val == null && vn == null) {
						// ok
					}
					else if (!val.equals(vn)) {
						throw new IllegalStateException("Parallel result reduction has failed");
					}
				}
				return val;
			}
		}
	}
}
