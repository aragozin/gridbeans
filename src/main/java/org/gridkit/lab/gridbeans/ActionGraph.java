package org.gridkit.lab.gridbeans;

import java.lang.reflect.Method;
import java.util.Set;

public interface ActionGraph {

	public Set<Action> allActions();
	
	public Set<Bean> allBeans();
	
	public Bean getNamed(Object name);
	
	public Set<ActionSite> allSites();

	public Set<ActionSite> allSites(Method method);

	public Set<Action> allActions(ActionSite site);

	public Set<Action> allActions(Bean bean, Method method);
	
	public Set<Action> allConsumer(Bean bean);

	public void addDependency(Action from, Action to);

	public void removeDependency(Action from, Action to);

	public Set<Action> getInitialActions();

	public Set<Action> getUpstream(Action action);

	public Set<Action> getDownstream(Action action);

	public Set<Action> getTerminalActions();
	
	/**
	 * Remove alias and replace its usage with primary.
	 * bean1 should be an external bean.
	 */
	public void unify(Bean alias, Bean primary);

	/**
	 * Unifies two actions. Action should be identical. 
	 * Intended to be used after bean unification. If action is producing bean,
	 * beans also going to be unified.
	 */
	public void unify(Action twin1, Action twin2);

	/**
	 * Removes action and unifies its result with provided bean.
	 */
	public void unify(Action action, Bean bean);
	
	/**
	 * Removes action. Action's result should be used by other action in graph.
	 * If call site doesn't have other actions it will be eliminated too.
	 */
	public void eliminate(Action action);
	
	public static interface Bean {

		Class<?> getType();
		
	}

	public static interface ActionSite {
		
		/** Global chronological sequence number */
		public int getSeqNo();
		
		public Method getMethod();

		/**
		 * Runtime method may be not accurate due to nature of proxy class.
		 * This method returns all alternative method declaration.
		 */
		public Set<Method> allMethodAliases();
		
		public StackTraceElement[] getStackTrace();		
	}
	
	public static interface ExternalBean extends Bean {
		
		public Object getName();
		
	}
	
	public static interface LocalBean extends Bean {
		
		public Action getOrigin();
		
	}
	
	public static interface Action {

		public ActionSite getSite();
		
		public Bean getHostBean();
		
		public Bean getResultBean();
		
		public Object[] getGroundParams();
		
		public Bean[] getBeanParams();
		
	}
	
	public static interface TrackingObserver {
		
		public void afterAction(ActionGraph graph, ActionSite site);
		
	}
	
	public static interface BeanResolver {
		
		public Bean resolve(ActionGraph graph, Object runtimeProxy);
		
	}
}
