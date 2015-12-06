package org.gridkit.lab.gridbeans.monadic;

import java.util.concurrent.TimeUnit;

public class MonadicExample {

    public void test() {
        
        MonadBuilder builder = null;
        Checkpoint lInit = builder.checkpoint("init"); 
        Checkpoint lStart = builder.checkpoint("start"); 
        Checkpoint lStop = builder.checkpoint("stop"); 
        Checkpoint lDone = builder.checkpoint("done"); 
        
        Cloud cloud = builder.locator(Cloud.class);
        MeteringDriver metering = builder.bean(MeteringDriver.class);
        
        MyDriver driver = cloud.at("WORKER").deploy(MyDriver.class, new MyDriverImpl());
        ExecutionDriver exec = cloud.at("WORKER").bean(ExecutionDriver.class);
        
        builder.rewind(lInit);
        Activity testTask = exec.newExecution(driver.createTask())
                .threadCount(10)
                .rateLimit(10)
                .report(metering.sampler().a("task", "myTask").intervalSampler())
                .start();
                
        builder.join(lStart);
        
        builder.wallclock().delay(30, TimeUnit.SECONDS);
        
        builder.join(lStop);
        
        testTask.stop();
        builder.join(testTask);
        
        builder.join(lDone);
    }
 
    public interface Cloud extends Locator {
        
        public ExecutionTarget at(String filter);
    }

    
    public interface MyDriver {

        public Runnable createTask();        
    }
    
    public static class MyDriverImpl implements MyDriver {

        @Override
        public Runnable createTask() {
            // TODO Auto-generated method stub
            return null;
        }
    }
    
    public interface ExecutionDriver {
        
        public Execution newExecution(Runnable task);
        
    }
    
    public interface Execution {
        
        public Execution threadCount(int thread);

        public Execution rateLimit(double rate);
        
        public Execution report(IntervalSampler sampler);
        
        public Activity start();

        public Activity slowStart(double rate);
    }
    
    public interface MeteringDriver {
        
        public SampleBuilder sampler();
        
        public void dumpMetering(MeteringSink sink);
    }

    public interface MeteringSink {
        
    }
    
    public interface SampleBuilder {
        
        /**
         * Add static attribute to resulting sample
         */
        public SampleBuilder a(String name, Object value);
        
        public EventSampler eventSampler();

        public ScalarSampler scalarSampler();

        public IntervalSampler intervalSampler();
    }
    
    public interface EventSampler {
        
        public void sample(long timestamp);
    }

    public interface ScalarSampler {
        
        public void sample(long timestamp, double value);
    }

    public interface IntervalSampler {
        
        public void sample(long timestamp, double duration);
    }
}
