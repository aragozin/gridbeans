package org.gridkit.lab.gridbeans.monadic.spi;

import java.lang.reflect.Method;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.BeanHandle;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.ExecutionHost;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.Invocation;
import org.gridkit.lab.gridbeans.monadic.RuntimeEnvironment.InvocationCallback;
import org.gridkit.lab.gridbeans.monadic.Wallclock;

public class ClockBean implements BeanHandle {

    private static final Method DELAY;
    static {
        try {
            DELAY = Wallclock.class.getMethod("delay", long.class, TimeUnit.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    private ScheduledExecutorService exec = new ScheduledThreadPoolExecutor(1);
    private ExecutionHost host;
    
    public ClockBean(ExecutionHost host) {
        this.host = host;
    }
    
    @Override
    public ExecutionHost getHost() {
        return host;
    }

    @Override
    public void fire(Invocation call, final InvocationCallback callback) {
        if (call.getMethod().equals(DELAY)) {
            long delay = (Long) call.getGroundParam(0);
            TimeUnit unit = (TimeUnit) call.getGroundParam(1);
            Runnable task = new Runnable() {
                @Override
                public void run() {
                    callback.done(null);
                }
            };
            exec.schedule(task, delay, unit);
        }
    }
}
