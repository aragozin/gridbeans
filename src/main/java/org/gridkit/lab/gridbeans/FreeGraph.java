package org.gridkit.lab.gridbeans;

import java.lang.ref.WeakReference;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An editable implementation of {@link ActionGraph}
 * 
 * @author Alexey Ragozin <alexey.ragozin@gmail.com>
 */
public class FreeGraph implements ActionGraph {
	
	private List<MappedBean> beans = new ArrayList<MappedBean>();
	private List<CopySite> sites = new ArrayList<CopySite>();
	private List<CopyAction> actions = new ArrayList<CopyAction>();
	
	private Map<Object, MappedBean> namedBeans = new HashMap<Object, MappedBean>();
	
	public FreeGraph() {		
	}
	
	public void merge(ActionGraph other) {
		
		MappingContext ctx = new MappingContext();
		Map<Action, CopyAction> mapping = new HashMap<Action, CopyAction>();
		
		for(ActionSite site: other.allSites()) {
			CopySite scopy = new CopySite(site);
			sites.add(scopy);
			for(Action action: other.allActions(site)) {
				CopyAction acopy = new CopyAction(scopy, action, ctx);
				actions.add(acopy);
				mapping.put(action, acopy);
			}
		}
		
		for(Action action: other.allActions()) {
			for(Action dep: other.getUpstream(action)) {
				addDependency(mapping.get(dep), mapping.get(action));
			}
		}

		remap: 
		while(true) {
			for(MappedBean mb: beans) {
				if (mb instanceof ExternalBean) {
					Object name = ((CopyExternalBean)mb).getName();
					MappedBean mb2 = namedBeans.get(name);
					if (mb2 == null) {
						namedBeans.put(name, mb);
					}
					else if (mb != mb2) {
						unify(mb, mb2);
						continue remap;
					}
				}
			}
			break;
		}		
	}
	
	private static interface Filter<T> {
		
		public boolean eval(T t);
		
	}	
	
	private Set<Action> searchActions(Filter<CopyAction> f) {
		Set<Action> result = new LinkedHashSet<Action>();
		for(CopyAction call: actions) {
			if (f.eval(call)) {
				result.add(call);
			}
		}
		return result;
	}

	private Set<Bean> searchBeans(Filter<MappedBean> f) {
		Set<Bean> result = new LinkedHashSet<Bean>();
		for(MappedBean bean: beans) {
			if (f.eval(bean)) {
				result.add(bean);
			}
		}
		return result;
	}

	private Set<ActionSite> searchSites(Filter<CopySite> f) {
		Set<ActionSite> result = new LinkedHashSet<ActionSite>();
		for(CopySite site: sites) {
			if (f.eval(site)) {
				result.add(site);
			}
		}
		return result;
	}
	
	private CopySite verify(ActionSite site) {
		if (site instanceof CopySite) {
			CopySite c = (CopySite) site;
			if (c.getHost() == this) {
				return c;
			}
		}
		throw new IllegalArgumentException("Site '" + site + "' does not belong to this graph");
	}

	private CopyAction verify(Action action) {
		if (action instanceof CopyAction) {
			CopyAction c = (CopyAction) action;
			if (c.getHost() == this) {
				return c;
			}
		}
		throw new IllegalArgumentException("Action '" + action + "' does not belong to this graph");
	}

	private MappedBean verify(Bean bean) {
		if (bean instanceof MappedBean) {
			MappedBean c = (MappedBean) bean;
			if (c.getHost() == this) {
				return c;
			}
		}
		throw new IllegalArgumentException("Bean '" + bean + "' does not belong to this graph");
	}
	
	@Override
	public Set<Action> allActions() {
		return searchActions(new Filter<CopyAction>() {
			@Override
			public boolean eval(CopyAction t) {
				return true;
			}
		});
	}

	@Override
	public Set<Bean> allBeans() {
		return searchBeans(new Filter<MappedBean>() {
			@Override
			public boolean eval(MappedBean t) {
				return true;
			}
		});
	}

	@Override
	public Set<ActionSite> allSites() {
		return searchSites(new Filter<CopySite>(){
			@Override
			public boolean eval(CopySite t) {
				return true;
			}
		});
	}

	@Override
	public Bean getNamed(Object name) {
		return namedBeans.get(name);
	}
	
	@Override
	public Set<ActionSite> allSites(final Method method) {
		return searchSites(new Filter<CopySite>(){
			@Override
			public boolean eval(CopySite t) {
				if (method != null) {
					return t.methodAliases.contains(method);
				}
				else {
					return true;
				}
			}
		});
	}

	@Override
	public Set<Action> allActions(final ActionSite site) {
		if (site != null) {
			verify(site);
		}
		return searchActions(new Filter<CopyAction>() {
			@Override
			public boolean eval(CopyAction t) {
				if (site != null) {
					return t.site == site;
				}
				return true;
			}
		});
	}

	@Override
	public Set<Action> allActions(final Bean bean, final Method method) {
		if (bean != null) {
			verify(bean);
		}
		return searchActions(new Filter<CopyAction>() {
			@Override
			public boolean eval(CopyAction t) {
				if (bean != null && t.getHostBean() != bean) {
					return false;
				}
				if (method != null && !t.site.methodAliases.contains(method)) {
					return false;
				}
				return true;
			}
		});
	}

	@Override
	public Set<Action> allConsumer(final Bean bean) {
		verify(bean);
		return searchActions(new Filter<CopyAction>() {
			@Override
			public boolean eval(CopyAction t) {
				return t.getHostBean() == bean && Arrays.asList(t.getBeanParams()).contains(bean);
			}
		});
	}

	@Override
	public void addDependency(Action from, Action to) {
		CopyAction a = verify(from);
		CopyAction b = verify(to);
		if (!b.dependencies.contains(a)) {
			b.dependencies.add(a);
		}		
	}

	@Override
	public void removeDependency(Action from, Action to) {
		CopyAction a = verify(from);
		CopyAction b = verify(to);
		b.dependencies.remove(a);		
	}

    @Override
	public Set<Action> getInitialActions() {
		return searchActions(new Filter<CopyAction>() {
			@Override
			public boolean eval(CopyAction t) {
				return t.dependencies.isEmpty();
			}
		});
	}

	@Override
	public Set<Action> getUpstream(Action action) {
		final CopyAction a = verify(action);
		Set<Action> result = new LinkedHashSet<Action>();
		result.addAll(a.dependencies);
		return result;
	}

	@Override
	public Set<Action> getDownstream(Action action) {
		final CopyAction a = verify(action);
		return searchActions(new Filter<CopyAction>() {

			@Override
			public boolean eval(CopyAction t) {
				return t.dependencies.contains(a);
			}
		});
	}

	@Override
	public Set<Action> getTerminalActions() {
		Set<Action> result = new LinkedHashSet<Action>();
		for(CopyAction call: actions) {
			result.removeAll(call.dependencies);
		}
		return result;
	}

	@Override
	public void unify(Bean victim, Bean victor) {
		MappedBean mb1 = verify(victim);
		MappedBean mb2 = verify(victor);
		
		if (mb1 instanceof ExternalBean) {
			throw new IllegalArgumentException("Bean '" + mb1 + "' should be external");
		}
		if (mb2 instanceof ExternalBean) {
			throw new IllegalArgumentException("Bean '" + mb2 + "' should be external");
		}

		if (mb1.getType() != mb2.getType()) {
			throw new IllegalArgumentException("Bean type mismatch " + mb1.getType().getName() + " != " + mb2.getType().getName());
		}

		replaceBean(mb1, mb2);
		
		namedBeans.remove(((ExternalBean)mb1).getName());
		namedBeans.put(((ExternalBean)mb2).getName(), mb2);
		beans.remove(mb1);
	}

	@Override
	public void unify(Action action1, Action action2) {
		CopyAction a1 = verify(action1);
		CopyAction a2 = verify(action2);
		if (a1 == a2) {
			throw new IllegalArgumentException("Actions are same");
		}
		if (a1.site != a2.site) {
			throw new IllegalArgumentException("Actions should belong to same site");
		}
		if (a1.hostBean != a2.hostBean) {
			throw new IllegalArgumentException("Actions be called on same bean");
		}
		if (!Arrays.equals(a1.groundParams, a2.groundParams) || !Arrays.equals(a1.beanParams, a2.beanParams)) {
			throw new IllegalArgumentException("Actions should have same arguments");
		}
		
		Bean r1 = a1.resultBean;
		Bean r2 = a2.resultBean;
		
		actions.remove(a2);
		
		for(CopyAction a: actions) {
			if (a.dependencies.contains(a2)) {
				a.dependencies.remove(a2);
				if (!a.dependencies.contains(a1)) {
					a.dependencies.add(a1);
				}
			}
		}
		
		for(CopyAction dep: a2.dependencies) {
			if (!a1.dependencies.contains(dep)) {
				a1.dependencies.add(dep);
			}
		}
		
		if (r1 != null) {
			beans.remove(r2);
			replaceBean(r2, r1);
		}
	}

	@Override
	public void unify(Action action, Bean bean) {
		CopyAction a = verify(action);
		MappedBean b = verify(bean);
		
		if (a.resultBean == null) {
			throw new IllegalArgumentException("Action does not produce bean to unify");
		}
		
		if (a.resultBean.type != b.type) {
			throw new IllegalArgumentException("Bean type mismatch " + a.resultBean.type.getName() + " != " + b.type.getName());
		}
		
		Bean r = a.resultBean;
		replaceBean(r, b);
		beans.remove(r);
		
		removeActionInternal(a);
	}

	@Override
	public void eliminate(Action action) {
		CopyAction a = verify(action);
		
		if (a.resultBean == null && allConsumer(a.resultBean).isEmpty()) {
			removeActionInternal(a);
			if (a.resultBean != null) {
				beans.remove(a.resultBean);
			}
		}
	}

	private void replaceBean(Bean mb1, Bean mb2) {
		for(CopyAction action: actions) {
			if (action.hostBean == mb1) {
				action.hostBean = (MappedBean) mb2;
			}
			for(int i = 0; i != action.beanParams.length; ++i) {
				if (action.beanParams[i] == mb1) {
					action.beanParams[i] = mb2;
				}
			}
		}
	}

	private void removeActionInternal(CopyAction a) {
		actions.remove(a);
		
		for(CopyAction x: actions) {
			x.dependencies.remove(a);
		}
		
		if (allActions(a.getSite()).isEmpty()) {
			sites.remove(a.site);
		}
	}

	private class Bound {
		
		public FreeGraph getHost() {
			return FreeGraph.this;
		}		
	}
	
	private class CopySite extends Bound implements ActionSite {

		private final int seqNo;
		private final Method method;
		private final Set<Method> methodAliases;
		private final StackTraceElement[] stackTrace;
		
		public CopySite(ActionSite site) {
			seqNo = site.getSeqNo();
			method = site.getMethod();
			methodAliases = site.allMethodAliases();
			stackTrace = site.getStackTrace();			
		}
		
		@Override
		public int getSeqNo() {
			return seqNo;
		}

		@Override
		public Method getMethod() {
			return method;
		}

		@Override
		public Set<Method> allMethodAliases() {
			return methodAliases;
		}

		@Override
		public StackTraceElement[] getStackTrace() {
			return stackTrace;
		}

		@Override
		public String toString() {
			return PrintHelper.toString(this);
		}
	}

	private class CopyAction extends Bound implements Action {
		
		private final CopySite site;
		private MappedBean hostBean;
		private MappedBean resultBean;
		private Object[] groundParams;
		private Bean[] beanParams;
		
		private List<CopyAction> dependencies = new ArrayList<CopyAction>();
		
		public CopyAction(CopySite site, Action action, MappingContext ctx) {
			this.site = site;
			this.hostBean = ctx.mapBean(action.getHostBean());
			this.resultBean = ctx.mapResultBean(action.getResultBean(), this);
			this.groundParams = action.getGroundParams();
			this.beanParams = new Bean[action.getBeanParams().length];
			for(int i = 0; i != beanParams.length; ++i) {
				Bean eb = action.getBeanParams()[i];
				if (eb != null) {
					this.beanParams[i] = ctx.mapBean(eb);
				}
			}
		}
		
		@Override
		public ActionSite getSite() {
			return site;
		}

		@Override
		public Bean getHostBean() {
			return hostBean;
		}

		@Override
		public Bean getResultBean() {
			return resultBean;
		}

		@Override
		public Object[] getGroundParams() {
			return groundParams;
		}

		@Override
		public Bean[] getBeanParams() {
			return beanParams;
		}

		@Override
		public String toString() {
			return PrintHelper.toString(this);
		}
	}
	
	private class MappedBean extends Bound implements Bean {
		
		@SuppressWarnings("unused")
		private WeakReference<Bean> sourceRef;
		private Class<?> type;
		
		public MappedBean(Bean external) {
			sourceRef = new WeakReference<Bean>(external);
			type = external.getType();
		}

		@Override
		public Class<?> getType() {
			return type;
		}
	}
	
	private class CopyExternalBean extends MappedBean implements ExternalBean {

		private Object name;
		private String caption;
		
		public CopyExternalBean(ExternalBean bean) {
			super(bean);
			name = bean.getName();
			caption = bean.toString();
		}
		
		@Override
		public Object getName() {
			return name;
		}
		
		@Override
		public String toString() {
			return caption;
		}
	}

	private class CopyLocalBean extends MappedBean implements LocalBean {

		private Action origin;
		private String caption;
		
		public CopyLocalBean(LocalBean bean) {
			super(bean);
			caption = bean.toString();
		}

		@Override
		public Action getOrigin() {
			return origin;
		}
		
		@Override
		public String toString() {
			return caption;
		}
	}
	
	private class MappingContext {
		
		Map<Bean, MappedBean> beanMap = new HashMap<Bean, MappedBean>();
	
		
		MappedBean mapBean(Bean external) {
			MappedBean mp = beanMap.get(external);
			if (mp == null) {
				if (external instanceof LocalBean) {
					mp = new CopyLocalBean((LocalBean) external);
					beanMap.put(external, mp);				
				}
				else {
					mp = new CopyExternalBean((ExternalBean) external);
					beanMap.put(external, mp);				
				}
				beans.add(mp);
			}		
			return mp;
		}

		MappedBean mapResultBean(Bean external, Action action) {
			CopyLocalBean lb = (CopyLocalBean) mapBean(external);
			lb.origin = action;
			return lb;
		}
	}
}