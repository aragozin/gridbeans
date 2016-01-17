package org.gridkit.lab.gridbeans;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Set;

import org.gridkit.lab.gridbeans.ActionGraph.ActionSite;

public class CallSite {

    public static ActionSite newSite(Class<?> type, String method, Class<?>... signature) {
        try {
            Method m = type.getMethod(method, signature);
            StackTraceElement[] ste = Thread.currentThread().getStackTrace();
            return new SynthSite(m, ste);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    private static class SynthSite implements ActionSite {

        private Method method;
        private StackTraceElement[] trace;
        
        public SynthSite(Method method, StackTraceElement[] trace) {
            this.method = method;
            this.trace = trace;
        }

        @Override
        public ActionGraph getGraph() {
            return null;
        }

        @Override
        public int getSeqNo() {
            return -1;
        }

        @Override
        public Method getMethod() {
            return method;
        }

        @Override
        public Method getMethod(Class<?> declaringClass) {
            try {
                return declaringClass.getMethod(method.getName(), method.getParameterTypes());
            } catch (SecurityException e) {
                throw new RuntimeException(e);
            } catch (NoSuchMethodException e) {
                throw new IllegalArgumentException(e.getMessage());
            }
        }

        @Override
        public Set<Method> allMethodAliases() {
            return Collections.singleton(method);
        }

        @Override
        public StackTraceElement[] getStackTrace() {
            return trace;
        }

        @Override
        public RuntimeException getStackTraceAsExcpetion() {
            RuntimeException r = new RuntimeException("Call site");
            r.setStackTrace(trace);
            return r;
        }
    }
}
