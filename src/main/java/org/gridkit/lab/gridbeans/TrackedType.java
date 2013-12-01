package org.gridkit.lab.gridbeans;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for tracking generic type information in runtime.
 *  
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
@SuppressWarnings({"rawtypes"})
class TrackedType {
	
	private boolean delayed = false;
	
	private Type lazyType;
	private TrackedType lazyScope;
	
	private Class rawType;
	private Map<TypeVariable<?>, TrackedType> variables = new HashMap<TypeVariable<?>, TrackedType>();
	
	public TrackedType(Class type) {
		rawType = type;
		bindVariables(type);
	}
	
	TrackedType(TrackedType scope, Method m, Object[] arguments) {
		variables.putAll(scope.variables);
		Type[] gt = m.getGenericParameterTypes();
		for(int i = 0; i != gt.length; ++i) {
			if (arguments[i] != null) {
				if (gt[i] instanceof ParameterizedType) {
					TrackedType dtype;
					dtype = new TrackedType(arguments[i].getClass());
					if (arguments[i] instanceof Class) {						
						dtype.variables.put(Class.class.getTypeParameters()[0], new TrackedType((Class)arguments[i]));
					}
					bindCallParams((ParameterizedType) gt[i], dtype);
				}
				else if (gt[i] instanceof TypeVariable<?>) {
					variables.put((TypeVariable<?>)gt[i], new TrackedType(arguments[i].getClass()));
				}
			}
		}		
	}

	TrackedType(ParameterizedType type, TrackedType scope) {
		rawType = (Class) type.getRawType();
		bindVariables(type, scope);
	}
	
	TrackedType(Type type, TrackedType scope, boolean delayed) {
		this.delayed = delayed;
		lazyType = type;
		lazyScope = scope;
		if (delayed) {
			delayed = true;
		}
		else {
			init();
		}
	}
	
	private void init() {
		delayed = false;
		if (lazyType instanceof Class) {
			rawType = (Class) lazyType;
			bindVariables((Class) lazyType);
		}
		else if (lazyType instanceof ParameterizedType) {
			ParameterizedType pt = (ParameterizedType) lazyType;
			rawType = (Class) pt.getRawType();
			bindVariables(pt, lazyScope);
		}
		else {
			throw new IllegalArgumentException("Unknown " + lazyType);
		}			
	}

	private void bindVariables(Class type) {
		for(Type tp : type.getGenericInterfaces()) {
			if (tp instanceof Class) {
				bindVariables((Class)tp);
			}
			else if (tp instanceof ParameterizedType) {
				bindVariables((ParameterizedType) tp, this);
			}
			else {
				throw new IllegalArgumentException("Unexpeted type " + tp);
			}
		}
		if (type.getSuperclass() != null) {
			Type tp = type.getGenericSuperclass();
			if (tp instanceof Class) {
				bindVariables((Class)tp);
			}
			else if (tp instanceof ParameterizedType) {
				bindVariables((ParameterizedType) tp, this);
			}
			else {
				throw new IllegalArgumentException("Unexpeted type " + tp);
			}
		}
	}
	
	private void bindVariables(ParameterizedType tp, TrackedType scope) {
		TypeVariable<?>[] vars = ((Class)tp.getRawType()).getTypeParameters();
		Type[] actual = tp.getActualTypeArguments();
		for(int i = 0; i != vars.length; ++i) {
			TypeVariable<?> v = vars[i];
			Type t = actual[i];
			if (t instanceof WildcardType) {
				t = ((WildcardType)t).getUpperBounds()[0];
			}
			variables.put(v, scope.resolve(t));
		}
		
		bindVariables((Class)tp.getRawType());
	}

	private void bindCallParams(ParameterizedType tp, TrackedType scope) {
		TypeVariable<?>[] vars = ((Class)tp.getRawType()).getTypeParameters();
		Type[] actual = tp.getActualTypeArguments();
		for(int i = 0; i != actual.length; ++i) {
			Type t = actual[i];
			if (t instanceof TypeVariable<?>) {
				TypeVariable<?> v = (TypeVariable<?>) t;
				TypeVariable<?> tv = vars[i];
				TrackedType tt = scope.tryResolve(tv);
				if (tt != null) {
					variables.put(v, tt);
				}
			}
		}
	}

	public Class getRawType() {
		if (delayed) {
			init();
		}
		return rawType;
	}
	
	public TrackedType resolve(Method m, Object... params) {
		if (delayed) {
			init();
		}
		
		TrackedType mtype = new TrackedType(this, m, params);
		return mtype.resolve(m.getGenericReturnType());
	}
	
	public TrackedType resolve(Type var) {
		if (delayed) {
			init();
		}
		if (var instanceof Class) {
			return new TrackedType((Class)var, null, true);
		}
		else if (var instanceof TypeVariable<?>) {
			TypeVariable<?> tv = (TypeVariable<?>) var;
			TrackedType tracked = variables.get(var);
			if (tracked == null) {
				throw new IllegalArgumentException("Failed to resolve type " + var + " @ " + tv.getGenericDeclaration());
			}
			return tracked;
		}
		else if (var instanceof ParameterizedType) {
			return new TrackedType(((ParameterizedType) var), this, true);
		}
		else {
			throw new IllegalArgumentException("Cannot resolve " + var);
		}
	}

	TrackedType tryResolve(Type var) {
		if (delayed) {
			init();
		}
		if (var instanceof Class) {
			return new TrackedType((Class)var, null, true);
		}
		else if (var instanceof TypeVariable<?>) {
			TrackedType tracked = variables.get(var);
			return tracked;
		}
		else if (var instanceof ParameterizedType) {
			return new TrackedType(((ParameterizedType) var), this, true);
		}
		else {
			return null;
		}
	}
	
	public String toString() {
		return rawType == null ? String.valueOf(lazyType) : rawType.getName();
	}
}
