package org.apache.flink.streaming.api.operators.aion.sampling;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.operators.aion.WindowSSlack;
import org.apache.flink.streaming.api.operators.aion.WindowSSlackManager;
import org.apache.flink.streaming.api.operators.aion.estimators.WindowSizeEstimator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.apache.flink.streaming.api.operators.aion.WindowSSlackManager.STATS_SIZE;

/**
 * This class determines sampling rates across for each substream.
 * <p>
 * It is possible to update the sampling values when real sampling rate values are collected.
 */

public abstract class AbstractSSlackAlg {

	protected static final Logger LOG = LoggerFactory.getLogger(AbstractSSlackAlg.class);

	protected final WindowSSlackManager windowSSlackManager;
	protected final WindowSizeEstimator srEstimator;
	/* Data Structures */
	protected final Map<WindowSSlack, SamplingPlan> samplingPlanMap;
	/* Utils */
	protected final Random random;
	/* Metrics */
	protected final Histogram sizeEstimationErrorHisto;
	protected final Histogram srEstimationErrorHisto;

	AbstractSSlackAlg(
			final WindowSSlackManager windowSSlackManager,
			final WindowSizeEstimator srEstimator) {
		this.windowSSlackManager = windowSSlackManager;
		this.srEstimator = srEstimator;

		this.samplingPlanMap = new HashMap<>();

		this.random = new Random();

		this.sizeEstimationErrorHisto = new DescriptiveStatisticsHistogram(STATS_SIZE);
		this.srEstimationErrorHisto = new DescriptiveStatisticsHistogram(STATS_SIZE);
	}

	public void initWindow(WindowSSlack windowSSlack) {
		SamplingPlan samplingPlan =
				new SamplingPlan(windowSSlack, windowSSlackManager.getNumberOfSSPerWindow());
		samplingPlanMap.put(windowSSlack, samplingPlan);

		if (windowSSlackManager.isWarmedUp()) {
			initiatePlan(windowSSlack, samplingPlan);
		}
	}

	public void updateAfterPurging(WindowSSlack windowSSlack, int localSSIndex) {
		SamplingPlan plan = samplingPlanMap.getOrDefault(windowSSlack, null);
		if (plan == null) {
			LOG.warn("Sampling plan is null for window {}.{}.", windowSSlack.getWindowIndex(), localSSIndex);
			return;
		}

		if (windowSSlackManager.isWarmedUp()) {
			sizeEstimationErrorHisto.update(
					(Math.abs(plan.getObservedEvents(localSSIndex) - windowSSlack.getObservedEvents(localSSIndex))));
			srEstimationErrorHisto.update(
					((long) (1000 * Math.abs(plan.getSamplingRate(localSSIndex) - windowSSlack.getSamplingRate(localSSIndex)))));
		}

		/* Purge the SS part of the plan. */
		plan.purgeSS(localSSIndex);
		if (!windowSSlackManager.isWarmedUp()) {
			return;
		}
		/* Estimate the newest values. */
		updatePlan(windowSSlack);
	}


	public boolean sample(WindowSSlack windowSSlack, int localSSIndex, long eventTime) {
		// Avoid sampling when warm-up is not done!
		if (!windowSSlackManager.isWarmedUp()) {
			return true;
		}
		// Watermark already emitted
		if (windowSSlackManager.getLastEmittedWatermark() >=
				windowSSlackManager.getSSDeadline(windowSSlack.getWindowIndex(), localSSIndex)) {

			if (windowSSlack.isPurged(localSSIndex)) {
				LOG.warn("Received event for purged window.");
			}
			return false;
		}
		return random.nextDouble() <= samplingPlanMap.get(windowSSlack).getSamplingRate(localSSIndex);
	}


	public final long emitWatermark() {

		long lastWatermark = windowSSlackManager.getLastEmittedWatermark();
		WindowSSlack currWindow = windowSSlackManager.getWindowSlack(lastWatermark);
		for(int localSSIndex = currWindow.getSSLocalIndex(lastWatermark);
			localSSIndex < windowSSlackManager.getNumberOfSSPerWindow();
			localSSIndex++) {
			long watermarkTarget = windowSSlackManager.getSSDeadline(currWindow.getWindowIndex(), localSSIndex);
			// We found the unprocessed sub-stream
			if (lastWatermark < watermarkTarget) {
				if (satisfySubstreamTarget(currWindow, localSSIndex)) {
					return watermarkTarget;
				} else {
					return -1;
				}
			}
		}
	   return -1;
	}

	public HistogramStatistics getSizeEstimationStatistics() {
		return sizeEstimationErrorHisto.getStatistics();
	}

	public HistogramStatistics getSREstimationStatistics() {
		return srEstimationErrorHisto.getStatistics();
	}


	protected abstract void initiatePlan(WindowSSlack windowSSlack, SamplingPlan samplingPlan);

	protected abstract void updatePlan(WindowSSlack windowSSlack);

	protected abstract boolean satisfySubstreamTarget(WindowSSlack windowSSlack, int localSSIndex);

}
