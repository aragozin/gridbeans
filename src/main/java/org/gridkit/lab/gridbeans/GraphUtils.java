package org.gridkit.lab.gridbeans;

import org.gridkit.lab.gridbeans.ActionGraph.Action;
import org.gridkit.lab.gridbeans.ActionGraph.ActionSite;

public class GraphUtils {

	public static String dump(ActionGraph graph) {
		
		StringBuilder sb = new StringBuilder();
		
		for(ActionSite site: graph.allSites()) {
			sb.append("[").append(String.format("%2d", site.getSeqNo())).append("] ");
			sb.append(site.getStackTrace()[0]);
			sb.append('\n');
			for(Action action: graph.allActions(site)) {
				sb.append("     ");
				sb.append(PrintHelper.getCallDescriction(action));
				sb.append('\n');
			}
			sb.append('\n');
		}
		
		return sb.toString();
	}	
}
