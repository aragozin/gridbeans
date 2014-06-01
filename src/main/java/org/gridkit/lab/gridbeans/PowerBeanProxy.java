package org.gridkit.lab.gridbeans;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * {@link PowerBeanProxy} is an advanced dynamic proxy execution handler
 * capable of tracking erased generic types at runtime.
 * 
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public class PowerBeanProxy implements InvocationHandler {

	public static <T> T powerProxy(InvocationProcessor handler, Class<T> type) {
		return type.cast(new PowerBeanProxy(handler, type).getProxy());
	}

	@SuppressWarnings("unchecked")
	public static <T, P extends T> T cloneProxy(P proxy, InvocationProcessor handler) {
		if (proxy instanceof ProxyBean) {
			return (T)((ProxyBean)proxy).clone(null, handler);
		}
		else {
			throw new IllegalArgumentException("Is not a PowerBeanProxy: " + proxy);
		}
	}
	
	public static InvocationProcessor getHandler(Object proxy) {
		if (proxy instanceof ProxyBean) {
			return ((ProxyBean)proxy).getHandler(null);
		}
		else {
			return null;
		}
	}

	public static Collection<Class<?>> getFacade(Object proxy) {
		if (proxy instanceof ProxyBean) {
			return ((ProxyBean)proxy).getFacade(null);
		}
		else {
			return null;
		}
	}

	protected InvocationProcessor handler;
	protected ProxyBean proxy;
	protected Set<Class<?>> facade;
	protected TrackedType dynType; 
	
	protected PowerBeanProxy(InvocationProcessor handler, Class<?> intf) {
		this(handler, collectInterfaces(intf), new TrackedType(intf));
	}

	protected PowerBeanProxy(InvocationProcessor handler, TrackedType dtype) {
		this(handler, collectInterfaces(dtype.getRawType()), dtype);
	}
	
	protected PowerBeanProxy(InvocationProcessor handler, Collection<Class<?>> facade, TrackedType dType) {
		this.handler = handler;
		Class<?>[] type = new Class[facade.size() + 1];
		facade.toArray(type);
		type[type.length - 1] = ProxyBean.class;
		this.facade = new LinkedHashSet<Class<?>>(facade);
		this.dynType = dType;
		this.proxy = (ProxyBean) Proxy.newProxyInstance(type[0].getClassLoader(), type, this);
	}
	
	protected void setHandler(InvocationProcessor handler) {
		this.handler = handler;
	}
	
	@SuppressWarnings("unchecked")
	public <T> T getProxy() {
		return (T)proxy;
	}
	
	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		if (method.getDeclaringClass() == ProxyBean.class) {
			if ("clone".equals(method.getName())) {
				return new PowerBeanProxy((InvocationProcessor)args[1], dynType);
			}
			else if ("getFacade".equals(method.getName())) {
				return Collections.unmodifiableCollection(facade);
			}
			else {
				// TODO only one method in ProxyBean is assumed
				return handler;
			}
		}
		else if (method.getDeclaringClass() == Object.class) {
			return method.invoke(this, args);
		}
		
		Call call = new Call();
		call.callSite = new Exception();
		call.m = method;
		call.params = args;

		try {
			handler.process(call);
		}
		catch(Exception e) {
			throw new RuntimeException("InvocationProcessor error", e);
		}
		if (!call.done) {
			throw new RuntimeException("Return behavior is not set by InvocationProcessor");
		}
		else {
			if (call.returnException != null) {
				throw call.returnException;
			}
			else if (call.returnProxy != null) {
				PowerBeanProxy rproxy = createChainedProxy(call);
				return rproxy.getProxy();
			}
			else if (call.returnThis) {
				return proxy;
			}
			else {
				return call.returnValue;
			}
		}
	}

	protected PowerBeanProxy createDerivedProxy(InvocationProcessor processor, TrackedType type) {
		return new PowerBeanProxy(processor, type);
	}
	
	protected PowerBeanProxy createChainedProxy(Call call) {
		return createDerivedProxy(call.returnProxy, call.getCompileTimeReturnType());
	}

	@Override
	public String toString() {
		return "Proxy[" + handler.toString() + "]";
	}

	protected class Call implements Invocation {

		Exception callSite;
		Method m;
		Object[] params;
		TrackedType rType;
		Set<Method> ma;
		
		Object returnValue;
		Throwable returnException;
		boolean returnThis;
		InvocationProcessor returnProxy;
		boolean done;
		
		@Override
		public StackTraceElement getCallSite() {
			throw new UnsupportedOperationException();
		}

		@Override
		public StackTraceElement[] getWholeTrace() {
			return callSite.getStackTrace();
		}

		@Override
		public Method getMethod() {
			return m;
		}

		@Override
		public Object[] getArguments() {
			return params;
		}

		@Override
		public Class<?> getReturnType() {
			return getCompileTimeReturnType().getRawType();
		}
		
		@Override
		public TrackedType getCompileTimeReturnType() {
			if (rType == null) {
				rType = dynType.resolve(m, params);
			}
			return rType;
		}

		@Override
		public Set<Method> getAlternativeMethods() {
			if (ma == null) {
				ma = new HashSet<Method>();
				// TODO current method matching rules are ignoring generics and type coersion
				for(Class<?> c: facade) {
					for(Method mm : c.getMethods()) {
						if (m.getName().equals(mm.getName()) && Arrays.equals(m.getParameterTypes(), mm.getParameterTypes())) {
							ma.add(mm);
						}
					}
				}
			}
			return ma;
		}

		@Override
		public void doReturnObject(Object value) {
			if (done) {
				throw new IllegalStateException("return is already armed");
			}
			done = true;
			returnValue = value;
		}

		@Override
		public void doThrow(Throwable e) {
			if (done) {
				throw new IllegalStateException("return is already armed");
			}
			done = true;
			returnException = e;
		}

		@Override
		public void doReturnThis() {
			if (done) {
				throw new IllegalStateException("return is already armed");
			}
			done = true;
			returnThis = true;
		}

		@Override
		public void doReturnProxy(InvocationProcessor handler) {
			if (done) {
				throw new IllegalStateException("return is already armed");
			}
			done = true;
			returnProxy = handler;
		}
	}
	
	public interface InvocationProcessor {
		
		public void process(Invocation invocation);
		
	}
	
	public interface Invocation {
		
		public StackTraceElement getCallSite();
		
		public StackTraceElement[] getWholeTrace();
		
		public Method getMethod();
		
		public Class<?> getReturnType();

		/**
		 * May throw exception if compile time type cannot be infered
		 */
		public TrackedType getCompileTimeReturnType();
		
		public Set<Method> getAlternativeMethods();
		
		public Object[] getArguments();
		
		public void doReturnObject(Object value);
		
		public void doThrow(Throwable e);
		
		public void doReturnThis();
		
		public void doReturnProxy(InvocationProcessor handler);
				
	}
	
	private static interface ProxyBean {
		
		public PowerBeanProxy clone(ProxyBean signatureSafeGuard, InvocationProcessor handler);

		public InvocationProcessor getHandler(ProxyBean signatureSafeGuard);

		public Collection<Class<?>> getFacade(ProxyBean signatureSafeGuard);		
	}
	
	static List<Class<?>> collectInterfaces(Class<?> type) {		
		List<Class<?>> result = new ArrayList<Class<?>>();
		if (type.isInterface()) {
			result.add(type);
		}
		for(Class<?> i: type.getInterfaces()) {
			for(Class<?> ii: collectInterfaces(i)) {
				if (!result.contains(ii)) {
					result.add(ii);
				}
			}
		}
		if (type.getSuperclass() != null && type.getSuperclass() != Object.class) {
			for(Class<?> i: collectInterfaces(type.getSuperclass())) {
				if (!result.contains(i)) {
					result.add(i);
				}
			}
		}
		return result;
	}	
}
