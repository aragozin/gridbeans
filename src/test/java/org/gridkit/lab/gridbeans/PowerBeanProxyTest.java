package org.gridkit.lab.gridbeans;

import org.gridkit.lab.gridbeans.PowerBeanProxy.Invocation;
import org.gridkit.lab.gridbeans.PowerBeanProxy.InvocationProcessor;
import org.junit.Assert;
import org.junit.Test;


public class PowerBeanProxyTest {

	@Test
	public void verify_dynamic_type_tracking() {

		AutoHandler handler = new AutoHandler();
		HolderDriver d = PowerBeanProxy.powerProxy(handler, HolderDriver.class);
		
		d.echo("test");
		Holder h1 = d.newHolder();
		Holder h2 = d.newDynamic();
		Holder h3 = d.newSuperDynamic(Holder.class);
		
		h1.toString();
		h2.toString();
		h3.toString();
		
		Assert.assertSame(handler, PowerBeanProxy.getHandler(h1));
		Assert.assertSame(handler, PowerBeanProxy.getHandler(h2));
		Assert.assertSame(handler, PowerBeanProxy.getHandler(h3));

		Assert.assertArrayEquals(new Object[]{Holder.class}, PowerBeanProxy.getFacade(h1).toArray());
		Assert.assertArrayEquals(new Object[]{Holder.class}, PowerBeanProxy.getFacade(h2).toArray());
		Assert.assertArrayEquals(new Object[]{Holder.class}, PowerBeanProxy.getFacade(h3).toArray());
		
	}
	
	public static class AutoHandler implements InvocationProcessor {

		@Override
		public void process(Invocation invocation) {
			if (invocation.getReturnType() == void.class) {
				invocation.doReturnObject(null);
			}
			else {
				invocation.doReturnProxy(this);
			}			
		}
	}
	
	public static interface Driver<T> {
		
		public void run();
		
		public void echo(Object param);
		
		public Holder newHolder();
		
		public T newDynamic();
		
		public <X> X newSuperDynamic(Class<X> type);
		
	}

	public static interface HolderDriver extends Driver<Holder> {
		
	}
	
	public static interface Holder {
		
	}	

}
