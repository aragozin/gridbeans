package org.gridkit.lab.gridbeans;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import org.gridkit.vicluster.ViExecutor;

public class BeanHelper {

	public static <T> T lump(T... proxies) {
		return lump(Arrays.asList(proxies));
	}

	public static <T> T lump(Collection<T> proxies) {
		return ParallelBeanProxy.lump(proxies);
	}
	
	public static <T> T proxy(Class<T> type, AsyncBeanHandler... beans) {
		return ParallelBeanProxy.proxy(type, beans);
	}

	public static <T> T proxy(Class<T> type, Collection<AsyncBeanHandler> beans) {
		return ParallelBeanProxy.proxy(type, beans);		
	}
	
	public static <T> T directProxy(Class<T> type, T... beans) {
		return ParallelBeanProxy.directProxy(type, Arrays.asList(beans));
	}

	public static <T> T parallelProxy(Class<T> type, Executor exec, T... beans) {
		return ParallelBeanProxy.parallelProxy(type, exec, beans);
	}

	public static <T> T directProxy(Class<T> type, Collection<T> beans) {
		return ParallelBeanProxy.directProxy(type, beans);
	}
	
	public static <T> T remoteProxy(Class<T> type, ViExecutor host, Callable<T> factory) {
		List<AsyncBeanHandler> handlers = createRemoteHandlers(type, host, factory);
		return proxy(type, handlers);
	}


	public static <T> T remoteParallelProxy(Class<T> type, Executor exec, ViExecutor host, Callable<T> factory) {
		List<AsyncBeanHandler> handlers = createRemoteHandlers(type, host, factory);
		for(int i = 0; i != handlers.size(); ++i) {
			handlers.set(i, new AsyncBeanHandler.OffThreadAdapter(handlers.get(i), exec));
		}
		return proxy(type, handlers);
	}
	
	private static <T> List<AsyncBeanHandler> createRemoteHandlers(final Class<T> type, ViExecutor host, final Callable<T> factory) {
		List<AsyncBeanHandler> handlers = host.massExec(new Callable<AsyncBeanHandler>() {
			@Override
			public AsyncBeanHandler call() throws Exception {
				return new RemoteBeanHandler(new AsyncBeanHandler.DirectHandler(type.cast(factory.call())));
			}			
		});
		return handlers;
	}
}
