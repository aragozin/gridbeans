package org.gridkit.lab.gridbeans;

import org.gridkit.lab.gridbeans.ActionGraph.Action;
import org.gridkit.lab.gridbeans.ActionGraph.ActionSite;

class PrintHelper {

	public static String toString(ActionSite site) {
		return getShortDescriptions(site) + getLocation(site);
	}

	public static String getShortDescriptions(ActionSite site) {
		return "[" + site.getSeqNo() + "] " + site.getMethod().getDeclaringClass().getSimpleName() + "." + site.getMethod().getName();
	}
	
	public static String getLocation(ActionSite site) {
		if (site.getStackTrace().length > 0) {
			StackTraceElement e = site.getStackTrace()[0];
//			String location = " @ " + e.getMethodName() 
//					+ (e.isNativeMethod() ? "(Native Method)" :						                (e.getFileName() != null && e.getLineNumber() >= 0 ?
//					"(" + e.getFileName() + ":" + e.getLineNumber() + ")" :
//					(e.getFileName() != null ?  "("+e.getFileName()+")" : "(Unknown Source)")));
//			return location;
			return " @ " + e.toString();
		}
		else {
			return "";
		}
	}

	
	public static String toString(Action a) {
		StringBuilder sb = new StringBuilder();
		sb.append(getShortDescriptions(a.getSite()));
		sb.append(" | ").append(a.getHostBean());
		if (a.getGroundParams().length == 0) {
			sb.append(" <- ()");
		}
		else {
			sb.append(" <- (").append(getParamsDescription(a)).append(')');
		}
		sb.append(getLocation(a.getSite()));
		return sb.toString();
	}

	public static String getCallDescriction(Action a) {
		StringBuilder sb = new StringBuilder();
		sb.append(a.getResultBean()).append(" <- ");
		sb.append(a.getHostBean()).append(".");
		sb.append(a.getSite().getMethod().getName());
		if (a.getGroundParams().length == 0) {
			sb.append("()");
		}
		else {
			sb.append("(").append(getParamsDescription(a)).append(')');
		}
		return sb.toString();
	}

	private static Object getParamsDescription(Action a) {
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i != a.getBeanParams().length; ++i) {
			if (sb.length() > 0) {
				sb.append(", ");
			}
			if (a.getBeanParams()[i] != null) {
				sb.append(a.getBeanParams()[i]);
			}
			else {
				sb.append(String.valueOf(a.getGroundParams()[i]));
			}
		}
		return sb.toString();
	}
	
	
}
