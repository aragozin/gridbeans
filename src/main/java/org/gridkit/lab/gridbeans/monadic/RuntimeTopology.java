package org.gridkit.lab.gridbeans.monadic;

import java.lang.reflect.Method;
import java.util.Set;

public interface RuntimeTopology {

    TopologyNode root();
    
    public interface TopologyNode {

        public Set<? extends TopologyNode> resolveLocator(Method method, Object[] params);

        public boolean checkBean(Class<?> type, Object[] identity);

    }
}
