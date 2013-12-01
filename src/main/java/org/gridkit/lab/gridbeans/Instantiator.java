package org.gridkit.lab.gridbeans;

import java.lang.reflect.Method;

public interface Instantiator {

	public <T> T factoryCall(Method m, Object... arguments);
	
	public <T> T newInstance(Class<?> type, Object... arguments);
	
}
