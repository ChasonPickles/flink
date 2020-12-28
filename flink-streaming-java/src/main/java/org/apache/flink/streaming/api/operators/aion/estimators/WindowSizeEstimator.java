package org.apache.flink.streaming.api.operators.aion.estimators;

import org.apache.flink.streaming.api.operators.aion.WindowSSlack;
import org.apache.flink.streaming.api.operators.aion.WindowSSlackManager;
import org.apache.flink.streaming.api.operators.aion.diststore.DistStoreManager;
import org.apache.flink.streaming.api.operators.aion.diststore.SSDistStore;

import java.util.Set;

/**
 * This class is responsible to estimate two main attributes:
 * 1) The target sampling rate for the window.
 * 2) The anticipated number of events in each substream.
 */

public class WindowSizeEstimator {

	private final WindowSSlackManager sSlackManager;
	private final DistStoreManager netDelayManager;
	private final DistStoreManager genDelayManager;

	public WindowSizeEstimator(
			final WindowSSlackManager sSlackManager,
			final DistStoreManager netDelayManager,
			final DistStoreManager genDelayManager) {
		this.sSlackManager = sSlackManager;
		this.netDelayManager = netDelayManager;
		this.genDelayManager = genDelayManager;
	}

	public long getEventsNumPerSS() {
		Set<SSDistStore> purgedNetDelay = netDelayManager.getPurgedData();
		Set<SSDistStore> purgedGenDelay = genDelayManager.getPurgedData();

		assert purgedGenDelay.size() == purgedNetDelay.size();

		double averageNetDelay = purgedNetDelay.stream().mapToDouble(SSDistStore::getMean).average().getAsDouble();
		double averageGenDelay = purgedGenDelay.stream().mapToDouble(SSDistStore::getMean).average().getAsDouble();

		return (long) Math.ceil(sSlackManager.getSSLength() / averageGenDelay);
	}

	public long getEventsNumPerSS(WindowSSlack sSlack, long t) {
		Set<SSDistStore> purgedNetDelay = netDelayManager.getPurgedData();
		Set<SSDistStore> purgedGenDelay = genDelayManager.getPurgedData();

		assert purgedGenDelay.size() == purgedNetDelay.size();

		double averageNetDelay = purgedNetDelay.stream().mapToDouble(SSDistStore::getMean).average().getAsDouble();
		double averageGenDelay = purgedGenDelay.stream().mapToDouble(SSDistStore::getMean).average().getAsDouble();

		return (long) Math.ceil(sSlackManager.getSSLength() / averageGenDelay);
	}
}
