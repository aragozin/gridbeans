package org.gridkit.lab.gridbeans;

import static org.gridkit.nanocloud.VX.ISOLATE;

import java.util.Arrays;
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

	public Cloud cloud = CloudFactory.createCloud();
	
	public Executor exec = Executors.newCachedThreadPool();
	
	@After
	public void destroyExecutor() {
		((ExecutorService)exec).shutdown();
	}	
	
	@Before
	public void initCloud() {
		cloud.node("**").x(ISOLATE).setIsolateNodeType();
	}
	
	public AsyncBeanHandler exportRemote(String node, final String prefix) {
		return cloud.node(node).exec(new Callable<AsyncBeanHandler>() {
			@Override
			public AsyncBeanHandler call() throws Exception {				
				return new RemoteBeanHandler(new AsyncBeanHandler.DirectHandler(new SimpleTestDriver(prefix)));
			}
		});
	}
	
	@Test
	public void test_parallel_remote() {
	
		AsyncBeanHandler b1 = exportRemote("A", "");
		AsyncBeanHandler b2 = exportRemote("B", "");
		
		TestDriver td = ParallelBeanProxy.parallelProxy(TestDriver.class, exec, Arrays.asList(b1, b2));

		td.print("Hallo world!");
		Assert.assertEquals("123", td.echo("123"));
		Assert.assertEquals("ABC-123", td.withPrefix("ABC-").echo("123"));		
	}

	public static interface TestDriver {
		
		public void print(String text);
		
		public String echo(String text);
		
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
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			System.out.println("Waked");
		}

		@Override
		public String echo(String text) {
			System.out.println("[ECHO] " + prefix + text);			
			return prefix + text;
		}

		@Override
		public TestDriver withPrefix(String prefix) {
			return new SimpleTestDriver(this.prefix + prefix);
		}
	}
}
