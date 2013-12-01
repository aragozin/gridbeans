package org.gridkit.lab.gridbeans;

public interface ProtoProxy {

	/**
	 * Lumps several beans proxies with same types from same graph.
	 * @return lumped bean included this and other proxies supplied in params.
	 */
	public <T> T lump(Object... proxies);
	
}
