package org.gridkit.lab.gridbeans;

import static org.gridkit.nanocloud.VX.TYPE;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.gridkit.nanocloud.Cloud;
import org.gridkit.nanocloud.CloudFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RemoteBeanTest {

	public Executor exec = Executors.newCachedThreadPool();
	public Cloud cloud = CloudFactory.createCloud();
	
	@Before
	public void before() {
		cloud.node("**").x(TYPE).setIsolate();
	}
	
	@After
	public void destroyExecutor() {
		((ExecutorService)exec).shutdown();
	}

	@After
	public void destroyCloud() {
		cloud.shutdown();
	}
	
	@Test
	public void test_simple_bean() {
		cloud.nodes("a", "b");
		
		TestDriver td = BeanHelper.remoteProxy(TestDriver.class, cloud.node("**"), new Callable<TestDriver>() {

			@Override
			public TestDriver call() throws Exception {
				return new SimpleTestDriver("");
			}
			
		});
		
		td.print("Hallo world!");
		Assert.assertEquals("123", td.echo("123"));
		Assert.assertEquals("ABC-123", td.withPrefix("ABC-").echo("123"));
		Assert.assertEquals("123", td.doubleEcho("123"));
	}	

	@Test
	public void test_parallel_bean() {
		cloud.nodes("a", "b");
		
		TestDriver td = BeanHelper.remoteParallelProxy(TestDriver.class, exec, cloud.node("**"), new Callable<TestDriver>() {
			
			@Override
			public TestDriver call() throws Exception {
				return new SimpleTestDriver("");
			}
			
		});
		
		td.print("Hallo world!");
		Assert.assertEquals("123", td.echo("123"));
		Assert.assertEquals("ABC-123", td.withPrefix("ABC-").echo("123"));
		Assert.assertEquals("123", td.doubleEcho("123"));
	}	

	public static interface TestDriver {
		
		public void print(String text);
		
		public String echo(String text);

		public String doubleEcho(String text);
		
		public TestDriver withPrefix(String prefix);
		
	}
	
	public static class SimpleTestDriver implements TestDriver {
		
		private String prefix;

		public SimpleTestDriver(String prefix) {
			this.prefix = prefix;
		}

		@Override
		public void print(String text) {
			System.out.println("[PRINT] " + prefix + text);			
		}

		@Override
		public String echo(String text) {
			System.out.println("[ECHO] " + prefix + text);			
			return prefix + text;
		}

		@Override
		public String doubleEcho(String text) {
			System.out.println("[ECHO-1] " + prefix + text);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// ignore
			}
			System.out.println("[ECHO-2] " + prefix + text);
			return prefix + text;
		}

		@Override
		public TestDriver withPrefix(String prefix) {
			return new SimpleTestDriver(this.prefix + prefix);
		}
	}
}
