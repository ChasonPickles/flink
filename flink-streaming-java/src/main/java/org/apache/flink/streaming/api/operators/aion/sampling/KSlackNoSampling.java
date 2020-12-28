package org.apache.flink.streaming.api.operators.aion.sampling;

import org.apache.flink.streaming.api.operators.aion.WindowSSlack;
import org.apache.flink.streaming.api.operators.aion.WindowSSlackManager;
import org.apache.flink.streaming.api.operators.aion.estimators.WindowSizeEstimator;

public class KSlackNoSampling extends AbstractSSlackAlg {

	private long maxDelay;

	public KSlackNoSampling(
			final WindowSSlackManager sSlackManager,
			final WindowSizeEstimator srEstimator) {
		super(sSlackManager, srEstimator);
		this.maxDelay = 0;
	}

	@Override
	public void initiatePlan(WindowSSlack windowSSlack, SamplingPlan samplingPlan) {
		for (int i = 0; i < windowSSlackManager.getSSSize(); i++) {
			samplingPlan.updatePlan(i, Integer.MAX_VALUE, 1);
		}
	}

	public boolean sample(WindowSSlack windowSSlack, int localSSIndex, long eventTime) {
		this.maxDelay = Math.max(this.maxDelay, System.currentTimeMillis() - eventTime);
		// Avoid sampling when warm-up is not done!
		if (!windowSSlackManager.isWarmedUp()) {
			return true;
		}
		// Watermark already emitted
		if (windowSSlackManager.getLastEmittedWatermark() >=
				windowSSlackManager.getSSDeadline(windowSSlack.getWindowIndex(), localSSIndex)) {
			if (windowSSlack.isPurged(localSSIndex)) {
				LOG.warn("Attempting to add data to a purged window.");
			}
			return false;
		}
		return true;
	}

	@Override
	protected void updatePlan(WindowSSlack windowSSlack) {}

	@Override
	protected boolean satisfySubstreamTarget(WindowSSlack windowSSlack, int localSSIndex) {
		long watermarkTarget = windowSSlackManager.getSSDeadline(windowSSlack.getWindowIndex(), localSSIndex);
		return watermarkTarget + maxDelay > System.currentTimeMillis();
	}
}
