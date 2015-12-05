package org.gridkit.lab.gridbeans;

import java.lang.reflect.Method;
import java.util.Set;

import org.gridkit.lab.gridbeans.ActionGraph.Action;
import org.gridkit.lab.gridbeans.ActionGraph.ActionSite;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ActionTrackerQuickTest {

	private Method driver_run;
	private Method driver_echo;
	private Method driver_newHolder;
	private Method driver_newDynamic;
	private Method driver_newSuperDynamic;
	
	@Before
	public void init() throws SecurityException, NoSuchMethodException {
		driver_run = Driver.class.getDeclaredMethod("run");
		driver_echo = Driver.class.getDeclaredMethod("echo", Object.class);
		driver_newHolder = Driver.class.getDeclaredMethod("newHolder");
		driver_newDynamic = Driver.class.getDeclaredMethod("newDynamic");
		driver_newSuperDynamic = Driver.class.getDeclaredMethod("newSuperDynamic", Class.class);
	}
	
	@Test
	public void verify_trivial_graph_1() {
		
		ActionTracker tracker = new ActionTracker();
		
		Driver<?> d = tracker.inject("driver", Driver.class);
		
		d.run();
		
		ActionGraph graph = tracker.getGraph();
		
		Set<ActionSite> sites = graph.allSites();
		Assert.assertEquals(1, sites.size());
		
		ActionSite site = sites.iterator().next();
		
		Assert.assertEquals(0, site.getSeqNo());
		Assert.assertEquals(driver_run, site.getMethod());
		Assert.assertEquals(getClass().getName(), site.getStackTrace()[0].getClassName());
		
		Set<Action> actions = graph.allActions(null, null);
		
		Assert.assertEquals(1, actions.size());
		Action action = actions.iterator().next();
		
		Assert.assertEquals(driver_run, action.getSite().getMethod());
		Assert.assertEquals(0, action.getGroundParams().length);
		Assert.assertEquals(0, action.getBeanParams().length);

		Assert.assertEquals(tracker.proxy2bean(d), action.getHostBean());
	
		
		System.out.println(site);		
		System.out.println(action);		
	}

	@Test
	public void verify_trivial_graph_2() {
		
		ActionTracker tracker = new ActionTracker();
		
		Driver<?> d = tracker.inject("driver", Driver.class);
		
		d.echo("TEST");
		
		ActionGraph graph = tracker.getGraph();
		
		Set<ActionSite> sites = graph.allSites();
		Assert.assertEquals(1, sites.size());
		
		ActionSite site = sites.iterator().next();
		
		Assert.assertEquals(0, site.getSeqNo());
		Assert.assertEquals(driver_echo, site.getMethod());
		Assert.assertEquals(getClass().getName(), site.getStackTrace()[0].getClassName());
		
		Set<Action> actions = graph.allActions(null, null);
		
		Assert.assertEquals(1, actions.size());
		Action action = actions.iterator().next();
		
		Assert.assertEquals(driver_echo, action.getSite().getMethod());
		Assert.assertEquals(1, action.getGroundParams().length);
		Assert.assertEquals(1, action.getBeanParams().length);

		Assert.assertEquals("TEST", action.getGroundParams()[0]);
		Assert.assertEquals(null, action.getBeanParams()[0]);
		
		Assert.assertEquals(tracker.proxy2bean(d), action.getHostBean());
		
		
		System.out.println(site);		
		System.out.println(action);		
	}

	@Test
	public void verify_trivial_graph_3() {
		
		ActionTracker tracker = new ActionTracker();
		
		Driver<?> d = tracker.inject("driver", Driver.class);
		
		Holder h = d.newHolder();
		d.echo(h);
		
		ActionGraph graph = tracker.getGraph();
		
		Set<ActionSite> sites = graph.allSites();
		Assert.assertEquals(2, sites.size());
		
		
		Set<Action> actions = graph.allActions(null, null);
		
		Assert.assertEquals(2, actions.size());
		
		Action action1 = graph.allActions(null, driver_newHolder).iterator().next();
		Action action2 = graph.allActions(null, driver_echo).iterator().next();
		
		Assert.assertEquals(driver_newHolder, action1.getSite().getMethod());
		Assert.assertEquals(0, action1.getGroundParams().length);
		Assert.assertEquals(0, action1.getBeanParams().length);

		Assert.assertEquals(driver_echo, action2.getSite().getMethod());
		Assert.assertEquals(1, action2.getGroundParams().length);
		Assert.assertEquals(1, action2.getBeanParams().length);
		
		Assert.assertEquals(null, action2.getGroundParams()[0]);
		Assert.assertEquals(action1.getResultBean(), action2.getBeanParams()[0]);
		
		Assert.assertEquals(tracker.proxy2bean(d), action1.getHostBean());
		Assert.assertEquals(tracker.proxy2bean(d), action2.getHostBean());
		Assert.assertEquals(tracker.proxy2bean(h), action1.getResultBean());
		Assert.assertEquals(null, action2.getResultBean());
				
		System.out.println(action1);		
		System.out.println(action2);		
	}

	@Test
	public void verify_dynamic_type_tracking_1() {
		
		ActionTracker tracker = new ActionTracker();
		
		HolderDriver d = tracker.inject("driver", HolderDriver.class);
		
		Holder h = d.newDynamic();
		d.echo(h);
		
		ActionGraph graph = tracker.getGraph();
		
		Set<ActionSite> sites = graph.allSites();
		Assert.assertEquals(2, sites.size());
		
		
		Set<Action> actions = graph.allActions(null, null);
		
		Assert.assertEquals(2, actions.size());
		
		Action action1 = graph.allActions(null, driver_newDynamic).iterator().next();
		Action action2 = graph.allActions(null, driver_echo).iterator().next();
		
		Assert.assertEquals(driver_newDynamic, action1.getSite().getMethod());
		Assert.assertEquals(0, action1.getGroundParams().length);
		Assert.assertEquals(0, action1.getBeanParams().length);
		
		Assert.assertEquals(driver_echo, action2.getSite().getMethod());
		Assert.assertEquals(1, action2.getGroundParams().length);
		Assert.assertEquals(1, action2.getBeanParams().length);
		
		Assert.assertEquals(null, action2.getGroundParams()[0]);
		Assert.assertEquals(action1.getResultBean(), action2.getBeanParams()[0]);
		
		Assert.assertEquals(tracker.proxy2bean(d), action1.getHostBean());
		Assert.assertEquals(tracker.proxy2bean(d), action2.getHostBean());
		Assert.assertEquals(tracker.proxy2bean(h), action1.getResultBean());
		Assert.assertEquals(null, action2.getResultBean());
		
		System.out.println(action1);		
		System.out.println(action2);		
	}

	@Test
	public void verify_super_dynamic_type_tracking_1() {
		
		ActionTracker tracker = new ActionTracker();
		
		HolderDriver d = tracker.inject("driver", HolderDriver.class);
		
		Holder h = d.newSuperDynamic(Holder.class);
		d.echo(h);
		
		ActionGraph graph = tracker.getGraph();
		
		Set<ActionSite> sites = graph.allSites();
		Assert.assertEquals(2, sites.size());
		
		
		Set<Action> actions = graph.allActions(null, null);
		
		Assert.assertEquals(2, actions.size());
		
		Action action1 = graph.allActions(null, driver_newSuperDynamic).iterator().next();
		Action action2 = graph.allActions(null, driver_echo).iterator().next();
		
		Assert.assertEquals(driver_newSuperDynamic, action1.getSite().getMethod());
		Assert.assertEquals(1, action1.getGroundParams().length);
		Assert.assertEquals(1, action1.getBeanParams().length);
		
		Assert.assertEquals(driver_echo, action2.getSite().getMethod());
		Assert.assertEquals(1, action2.getGroundParams().length);
		Assert.assertEquals(1, action2.getBeanParams().length);
		
		Assert.assertEquals(null, action2.getGroundParams()[0]);
		Assert.assertEquals(action1.getResultBean(), action2.getBeanParams()[0]);
		
		Assert.assertEquals(tracker.proxy2bean(d), action1.getHostBean());
		Assert.assertEquals(tracker.proxy2bean(d), action2.getHostBean());
		Assert.assertEquals(tracker.proxy2bean(h), action1.getResultBean());
		Assert.assertEquals(null, action2.getResultBean());
		
		System.out.println(action1);		
		System.out.println(action2);		
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
