package org.gridkit.lab.gridbeans;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * 
 * 
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public class PowerBeanProxy {

	public static InvocationHandler getHandler(Object proxy) {
		if (proxy instanceof ProxyBean) {
			return ((ProxyBean)proxy).getHandler();
		}
		else {
			return null;
		}
	}
	
	
	public interface IvocationHandler {
		
		public void process(Invocation invocation) {
			
		}
		
	}
	
	public interface Invocation {
		
		public StackTraceElement getCallSite();
		
		public Method getMethod();
		
		public void setReturnObject();
		
		public void setException(Throwable e);
		
		public void setReturnThis();
		
		public void setReturnProxy(InvocationHandler handler)
				
	}
	
	private static interface ProxyBean {
		
		public InvocationHandler getHandler();
		
	}
}
