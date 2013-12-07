package org.gridkit.lab.gridbeans;

import org.junit.Assert;
import org.junit.Test;

public class ParallelBeanTest {

	@Test
	public void test_simple_bean() {
		SimpleTestDriver a = new SimpleTestDriver("");
		SimpleTestDriver b = new SimpleTestDriver("");
		
		TestDriver td = ParallelBeanProxy.directProxy(TestDriver.class, a, b);
		
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
