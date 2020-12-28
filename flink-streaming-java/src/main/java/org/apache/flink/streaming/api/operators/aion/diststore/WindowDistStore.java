package org.apache.flink.streaming.api.operators.aion.diststore;

import org.apache.flink.streaming.api.operators.aion.WindowSSlack;

public class WindowDistStore {

	private final WindowSSlack windowSSlack;
	private final SSDistStore[] ssStores;
	private DistStoreManager distStoreManager;

	WindowDistStore(
			final WindowSSlack windowSSlack,
			final DistStoreManager distStoreManager,
			final int ssSize) {
		this.windowSSlack = windowSSlack;
		this.distStoreManager = distStoreManager;

		this.ssStores = new SSDistStore[ssSize];
		for (int i = 0; i < ssSize; i++) {
			ssStores[i] =
					distStoreManager.getStoreType() == DistStoreManager.DistStoreType.NET_DELAY ?
							new NetDelaySSStore(this, i) : new GenDelaySSStore(this, i);
		}
	}

	long getWindowIndex() {
		return windowSSlack.getWindowIndex();
	}

	public void addEvent(int ssLocalIndex, long value) {
		ssStores[ssLocalIndex].addValue(value);
	}

	public boolean purgeSS(int ssLocalIndex) {
		if (!ssStores[ssLocalIndex].isPurged()) {
			ssStores[ssLocalIndex].purge();
			distStoreManager.addPurgedSS(ssStores[ssLocalIndex]);
			return true;
		}
		return false;
	}

	public boolean isPurged(int localSSIndex) {
		return ssStores[localSSIndex].isPurged();
	}
}
