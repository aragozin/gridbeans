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
	
    @Test
    public void verify_dynamic_factory_type_inference() {
        
        ActionTracker tracker = new ActionTracker();
        
        HolderDriver d = tracker.inject("driver", HolderDriver.class);
        
        Fabricator<Holder> f = d.newDynmaicFactory(Holder.class);
        Holder h = f.fabricate();

        Assert.assertNotNull(h);
    }

    @Test
    public void verify_indirect_dynamic_factory_type_inference() {
        
        ActionTracker tracker = new ActionTracker();
        
        HolderDriver d = tracker.inject("driver", HolderDriver.class);
        
        Fabricator<Holder> f = d.newIndirectDynmaicFactory(HolderMarker.class);
        Holder h = f.fabricate();
        
        Assert.assertNotNull(h);
    }

    // Dynamic interface upcast is not implemented
    //@Test
    public void verify_dynamic_interface_upcast() {
        
        ActionTracker tracker = new ActionTracker();
        
        HolderDriver d = tracker.inject("driver", HolderDriver.class);
        
        Holder h = d.passthough(new HolderImpl());
        
        Assert.assertNotNull(h);
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

		public <TT> TT passthough(TT bean);
		
		public <X> X newSuperDynamic(Class<X> type);
		
        public <X> Fabricator<X> newDynmaicFactory(Class<X> type);

        public <X> Fabricator<X> newIndirectDynmaicFactory(Class<? extends Marker<X>> type);
        
	}

	public static interface HolderDriver extends Driver<Holder> {
		
	}
	
	public static interface Holder {
	    
	}	

	public static class HolderImpl implements Holder {
	    
	}
	
    public static interface Marker<T> {
        
    }
    
    public static interface HolderMarker extends Marker<Holder> {
        
    }
    
    public static interface Fabricator<T> {
        
        public T fabricate();
        
    }
}
