package org.apache.flink.streaming.api.operators.aion.sampling;

import org.apache.flink.streaming.api.operators.aion.WindowSSlack;
import org.apache.flink.streaming.api.operators.aion.WindowSSlackManager;
import org.apache.flink.streaming.api.operators.aion.estimators.WindowSizeEstimator;

public class NaiveSSlackAlg extends AbstractSSlackAlg {

	private static final double TARGET_SR = 0.8;

	public NaiveSSlackAlg(
			final WindowSSlackManager sSlackManager,
			final WindowSizeEstimator srEstimator) {
		super(sSlackManager, srEstimator);
	}

	@Override
	public void initiatePlan(WindowSSlack windowSSlack, SamplingPlan samplingPlan) {
		long ssEventsNum = srEstimator.getEventsNumPerSS();
		LOG.info("initiatePlan: Anticipating {} events per SS", ssEventsNum);
		for (int i = 0; i < windowSSlackManager.getNumberOfSSPerWindow(); i++) {
			samplingPlan.updatePlan(i, ssEventsNum, TARGET_SR);
		}
	}

	@Override
	protected void updatePlan(WindowSSlack windowSSlack) {
		SamplingPlan plan = samplingPlanMap.get(windowSSlack);
		long ssEventsNum = srEstimator.getEventsNumPerSS();
		LOG.info("updatePlan: Anticipating {} events per SS", ssEventsNum);
		for (int i = 0; i < windowSSlackManager.getNumberOfSSPerWindow(); i++) {
			if (!plan.isSSPurged(i))
				plan.updatePlan(i, ssEventsNum, TARGET_SR);
		}
	}

	@Override
	protected boolean satisfySubstreamTarget(WindowSSlack windowSSlack, int localSSIndex) {
		SamplingPlan plan = samplingPlanMap.getOrDefault(windowSSlack, null);
		long watermarkTarget = windowSSlackManager.getSSDeadline(windowSSlack.getWindowIndex(), localSSIndex);
		return windowSSlackManager.getProcessingTimeService().getCurrentProcessingTime() >= watermarkTarget &&
				plan.getObservedEvents(localSSIndex) <= windowSSlack.getObservedEvents(localSSIndex);
	}

}
