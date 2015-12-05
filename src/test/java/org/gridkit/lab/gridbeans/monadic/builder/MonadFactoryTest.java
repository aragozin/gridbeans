package org.gridkit.lab.gridbeans.monadic.builder;

import junit.framework.Assert;

import org.gridkit.lab.gridbeans.monadic.ExecutionTarget;
import org.gridkit.lab.gridbeans.monadic.Locator;
import org.gridkit.lab.gridbeans.monadic.MonadBuilder;
import org.junit.Test;

public class MonadFactoryTest {

    @Test
    public void smoke() {
        
        MonadBuilder mb = MonadFactory.build();
        
        Cloud cloud = mb.locator(Cloud.class);
        MyDriver md = cloud.at("A").deploy(MyDriver.class, new MyBean());
        
        md.do1();
        mb.join(mb.label("cp1"));
        md.do2();
        mb.join(mb.label("cp2"));

        mb.rewind(mb.label("cp1"));
        md.do1();
        mb.sync();
        md.do2();
        mb.join(mb.label("cp2"));
        
        mb.finish();
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
    
    
    public interface MyDriver {
        
        public void do1();

        public void do2();
        
    }
    
    public class MyBean implements MyDriver {

        @Override
        public void do1() {
        }

        @Override
        public void do2() {
        }
    }
    
    public interface Cloud extends Locator {
        
        public ExecutionTarget at(String filter);
        
        public void badLocator();

        public ExecutionTarget badLocator(MyDriver md);
    }    
}
