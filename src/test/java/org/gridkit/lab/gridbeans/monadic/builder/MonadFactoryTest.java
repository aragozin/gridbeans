package org.gridkit.lab.gridbeans.monadic.builder;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.gridkit.lab.gridbeans.monadic.DeployerSPI;
import org.gridkit.lab.gridbeans.monadic.ExecutionTarget;
import org.gridkit.lab.gridbeans.monadic.Locator;
import org.gridkit.lab.gridbeans.monadic.MonadBuilder;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.BeanHandle;
import org.gridkit.lab.gridbeans.monadic.ScenarioDefinition;
import org.gridkit.lab.gridbeans.monadic.ScenarioDefinition.ExecutionClosure;
import org.gridkit.lab.gridbeans.monadic.Wallclock;
import org.gridkit.lab.gridbeans.monadic.spi.ClockBean;
import org.gridkit.lab.gridbeans.monadic.spi.DirectBeanHandle;
import org.gridkit.lab.gridbeans.monadic.spi.ExecutorDeployService;
import org.gridkit.lab.gridbeans.monadic.spi.GenericEnvironment;
import org.gridkit.lab.gridbeans.monadic.spi.GenericEnvironment.HostBuilder;
import org.gridkit.lab.gridbeans.monadic.spi.NullExecutionEnvironment;
import org.junit.Test;

import junit.framework.Assert;

public class MonadFactoryTest {

    @Test
    public void smoke() {
        
        MonadBuilder bld = MonadFactory.build();
        
        Cloud cloud = bld.locator(Cloud.class);
        MyDriver mdA = cloud.at("A").deploy(MyDriver.class, new MyBean());
        MyDriver mdB = cloud.at("B").deploy(MyDriver.class, new MyBean());
        
        // Exporting common stuff 2
        bld.publish(bld.deploy(CommonStuff.class , new Stuff()), 2);
        
        
        mdA.do1();
        bld.join(bld.checkpoint("cp1"));
        mdA.do2();
        bld.join(bld.checkpoint("cp2"));

        bld.rewind(bld.checkpoint("cp1"));
        mdA.do1();
        bld.sync();
        mdA.do2();
        bld.join(bld.checkpoint("cp2"));

        bld.rewind(bld.checkpoint("cp1"));
        mdB.do1();
        bld.sync();
        mdB.do2();
        bld.join(bld.checkpoint("cp2"));

        bld.rewind(bld.checkpoint("cp1"));
        mdA.do1();
        mdB.do1();
        bld.checkpoint();
        mdA.do2();
        mdB.do2();
        bld.join(bld.checkpoint("cp2"));
        
        bld.rewind();

        CommonStuff cs = bld.bean(CommonStuff.class);
        CommonStuff cs2 = bld.bean(CommonStuff.class, 2);
        mdA.init(cs);
        mdA.init(cs2);
        mdB.init(cs);
        bld.join(bld.checkpoint("cp1"));
        
        ScenarioDefinition m = bld.finish();
        
        NullExecutionEnvironment ne = new NullExecutionEnvironment();
        
        ne.root().createHost(Cloud.class).at("A");
        ne.root().createHost(Cloud.class).at("B");
        ExecutionClosure ec = m.bind(ne);
        ec.execute(new PrintObserver());
        new String();
    }

    @Test
    public void smoke_on_executor_target() {
        
        MonadBuilder mb = MonadFactory.build();
        
        Cloud cloud = mb.locator(Cloud.class);
        MyDriver md = cloud.at("A").deploy(MyDriver.class, new MyBean());
        MyDriver md2 = cloud.at("B").deploy(MyDriver.class, new MyBean());
        
        md.do1();
        mb.join(mb.checkpoint("cp1"));
        md.do2();
        mb.join(mb.checkpoint("cp2"));

        mb.rewind(mb.checkpoint("cp1"));
        md.do1();
        mb.sync();
        md.do2();
        mb.join(mb.checkpoint("cp2"));

        mb.rewind(mb.checkpoint("cp1"));
        md2.do1();
        mb.sync();
        md2.do2();
        mb.join(mb.checkpoint("cp2"));

        mb.rewind(mb.checkpoint("cp1"));
        md.do1();
        md2.do1();
        mb.checkpoint();
        md.do2();
        md2.do2();
        mb.join(mb.checkpoint("cp2"));

        mb.rewind(mb.checkpoint("cp1"));
        mb.wallclock().delay(1, TimeUnit.SECONDS);
        mb.join(mb.checkpoint("cp2"));
        
        mb.rewind();

        CommonStuff cs = mb.bean(CommonStuff.class);
        md.init(cs);
        md2.init(cs);
        mb.join(mb.checkpoint("cp1"));
        
        ScenarioDefinition m = mb.finish();
        
        Executor exec = Executors.newSingleThreadExecutor();
        
        GenericEnvironment gen = new GenericEnvironment();
        
        HostBuilder hostA = gen.createHost("A");
        HostBuilder hostB = gen.createHost("B");
        
        gen.rootBuilder().injectLocation(hostA.getHost(), Cloud.class).at("A");
        gen.rootBuilder().injectLocation(hostB.getHost(), Cloud.class).at("B");

        gen.rootBuilder().injectBean(new ClockBean(gen.root()), Wallclock.class);
        
        ExecutorDeployService depA = new ExecutorDeployService(hostA.getHost(), exec);
        ExecutorDeployService depB = new ExecutorDeployService(hostB.getHost(), exec);
        
        hostA.injectBean(depA, DeployerSPI.class);
        hostB.injectBean(depB, DeployerSPI.class);

        BeanHandle stuff = new DirectBeanHandle(null, new Stuff());
        hostA.injectBean(depA.wrap(stuff), CommonStuff.class);
        hostB.injectBean(depB.wrap(stuff), CommonStuff.class);
        
        ExecutionClosure ec = m.bind(gen);
        ec.execute(new PrintObserver());
        new String();
    }
    
    @Test
    public void fail_on_bad_locator() {
        
        MonadBuilder mb = MonadFactory.build();
        
        Cloud cloud = mb.locator(Cloud.class);
        cloud.at("A").deploy(MyDriver.class, new MyBean());

        try {
            cloud.badLocator();
            Assert.fail("Exception expected");
        }
        catch(IllegalArgumentException e) {
            Assert.assertEquals("Locator contract violation, call MUST return ExecutionTarget", e.getMessage());
        }
    }

    @Test
    public void fail_on_bad_locator2() {
        
        MonadBuilder mb = MonadFactory.build();
        
        Cloud cloud = mb.locator(Cloud.class);
        MyDriver md = cloud.at("A").deploy(MyDriver.class, new MyBean());
        
        try {
            cloud.badLocator(md);
            Assert.fail("Exception expected");
        }
        catch(IllegalArgumentException e) {
            Assert.assertEquals("Locator contract violation, locator cannot accept bean references as arguments", e.getMessage());
        }
    }

    @Test
    public void fail_on_bad_deployment_type() {
        
        MonadBuilder mb = MonadFactory.build();
        
        Cloud cloud = mb.locator(Cloud.class);
        
        try {
            cloud.at("A").deploy(MyBean.class, new MyBean());
            Assert.fail("Exception expected");
        }
        catch(IllegalArgumentException e) {
            Assert.assertEquals("Deploy type should be an interface, but class is passed [org.gridkit.lab.gridbeans.monadic.builder.MonadFactoryTest$MyBean]", e.getMessage());
        }
    }

    @Test
    public void fail_on_bad_deployment_bean() {
        
        MonadBuilder mb = MonadFactory.build();
        
        Cloud cloud = mb.locator(Cloud.class);
        
        MyDriver md = cloud.at("A").deploy(MyDriver.class, new MyBean());
        try {
            cloud.at("B").deploy(MyDriver.class, md);
            Assert.fail("Exception expected");
        }
        catch(IllegalArgumentException e) {
            Assert.assertEquals("Real object required for deployment", e.getMessage());
        }
    }

    @Test
    public void fail_on_null_deployment() {
        
        MonadBuilder mb = MonadFactory.build();
        
        Cloud cloud = mb.locator(Cloud.class);
        
        try {
            cloud.at("B").deploy(MyDriver.class, null);
            Assert.fail("Exception expected");
        }
        catch(IllegalArgumentException e) {
            Assert.assertEquals("null cannot be deployed", e.getMessage());
        }
    }

    @Test
    public void fail_on_bad_publish() {
        
        MonadBuilder mb = MonadFactory.build();
        
        Cloud cloud = mb.locator(Cloud.class);
        
        try {
            cloud.at("B").publish("myBean");
            Assert.fail("Exception expected");
        }
        catch(IllegalArgumentException e) {
            Assert.assertEquals("First argument should be bean created by this builder", e.getMessage());
        }
    }

    @Test
    public void fail_on_null_publish() {
        
        MonadBuilder mb = MonadFactory.build();
        
        Cloud cloud = mb.locator(Cloud.class);
        
        try {
            cloud.at("B").publish(null);
            Assert.fail("Exception expected");
        }
        catch(IllegalArgumentException e) {
            Assert.assertEquals("First argument should be bean created by this builder", e.getMessage());
        }
    }
    
    public interface CommonStuff {
        
    }
    
    public class Stuff implements CommonStuff {
        
    }
    
    public interface MyDriver {
        
        public void init(CommonStuff stuff);
        
        public void do1();

        public void do2();
        
    }
    
    public class MyBean implements MyDriver {

        @Override
        public void init(CommonStuff stuff) {
            System.out.println("  > init(" + stuff + ")");
        }

        @Override
        public void do1() {
            System.out.println("  > do1()");
        }

        @Override
        public void do2() {
            System.out.println("  > do2()");
        }
    }
    
    public interface Cloud extends Locator {
        
        public ExecutionTarget at(String filter);
        
        public void badLocator();

        public ExecutionTarget badLocator(MyDriver md);
    }    
}
