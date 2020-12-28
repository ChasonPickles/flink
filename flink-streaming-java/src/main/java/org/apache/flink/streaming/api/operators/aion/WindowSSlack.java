package org.apache.flink.streaming.api.operators.aion;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.operators.aion.diststore.WindowDistStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.streaming.api.operators.aion.WindowSSlackManager.MAX_NET_DELAY;
import static org.apache.flink.streaming.api.operators.aion.WindowSSlackManager.STATS_SIZE;

public class WindowSSlack {

	protected static final Logger LOG = LoggerFactory.getLogger(WindowSSlack.class);

	/* Identifiers for WindowSS */
	private final long windowIndex;
	private final WindowSSlackManager sSlackManager;

	/* Stores */
	private final WindowDistStore netDelayStore;
	private final WindowDistStore genDelayStore;

	private final long[] sampledEvents;
	private final long[] shedEvents;

	/* Metrics */
	private final Histogram eventsPerSSHisto;
	private final Histogram samplingRatePerSSHisto;

	WindowSSlack(
			/* Identifiers */
			final WindowSSlackManager sSlackManager,
			final long windowIndex) {
		this.windowIndex = windowIndex;
		this.sSlackManager = sSlackManager;

		this.netDelayStore = sSlackManager.getNetDelayStoreManager().createWindowDistStore(this);
		this.genDelayStore = sSlackManager.getInterEventStoreManager().createWindowDistStore(this);

		this.sampledEvents = new long[sSlackManager.getSSSize()];
		this.shedEvents = new long[sSlackManager.getSSSize()];

		this.eventsPerSSHisto = new DescriptiveStatisticsHistogram(STATS_SIZE);
		this.samplingRatePerSSHisto = new DescriptiveStatisticsHistogram(STATS_SIZE);
	}

	/*
	 * Internal function that @returns local substream index in relation to window.
	 */
	public int getSSLocalIndex(long eventTime) {
		assert sSlackManager.getWindowIndex(eventTime) == windowIndex;
		return (int) ((eventTime - (windowIndex * sSlackManager.getWindowLength())) / sSlackManager.getSSLength());
	}

	/*
	 * Public interface that determines to sample the tuple or not.
	 *
	 * @returns a boolean value that determines if the tuple to be included in the sample.
	 */
	public boolean sample(long eventTime) {
		int localSSIndex = getSSLocalIndex(eventTime);
		long delay = sSlackManager.getProcessingTimeService().getCurrentProcessingTime() - eventTime;

		/* In the case of extreme network delay, we do not consider such events. */
		if (delay > MAX_NET_DELAY) {
			return false;
		}

		netDelayStore.addEvent(localSSIndex, delay);
		genDelayStore.addEvent(localSSIndex, eventTime);

		/* Consider the algorithm's wise opinion. */
		if (sSlackManager.getsSlackAlg().sample(this, localSSIndex, eventTime)) {
			sampledEvents[localSSIndex]++;
			return true;
		}
		shedEvents[localSSIndex]++;
		return false;
	}

	/*
	 * Public interface that determines to sample the tuple or not.
	 *
	 * @returns a boolean value that determines if the tuple to be included in the sample.
	 */
	public long emitWatermark() {
		long watTime = sSlackManager.getsSlackAlg().emitWatermark();
		if (watTime != -1) {
			sSlackManager.recordWatermark(watTime);
		}
		return watTime;
	}

	boolean purgeSS(long maxPurgeTime) {
		boolean succPurged = false;
		/* Loop through subsamples deadlines */
		for (long time = windowIndex * sSlackManager.getWindowLength();
			 time <= maxPurgeTime;
			 time += sSlackManager.getSSLength()) {

			int localSSIndex = getSSLocalIndex(time);
			boolean newlyPurged = netDelayStore.purgeSS(localSSIndex) && genDelayStore.purgeSS(localSSIndex);

			if (newlyPurged) {
				long observedEvents = getObservedEvents(localSSIndex);
				double samplingRatio = getSamplingRate(localSSIndex);
				sSlackManager
						.getsSlackAlg()
						.updateAfterPurging(this, localSSIndex);

				LOG.info(
						"Purging {}.{}: [sampled: {}, discarded: {}, total: {}, sr: {}",
						windowIndex, localSSIndex, getSampledEvents(localSSIndex), shedEvents[localSSIndex],
						observedEvents, samplingRatio);

				if (sSlackManager.isWarmedUp()) {
					eventsPerSSHisto.update(observedEvents);
					samplingRatePerSSHisto.update((long) (samplingRatio * 1000));
				}
			}
			succPurged |= newlyPurged;
		}
		return succPurged;
	}

	public long getWindowIndex() {
		return windowIndex;
	}

	/* Manipulation functions for book-keept data */
	public boolean isPurged(int localSSIndex) {
		return netDelayStore.isPurged(localSSIndex);
	}

	private long getObservedEvents() {
		long sum = 0;
		for (int i = 0; i < sSlackManager.getSSSize(); i++) {
			sum += getObservedEvents(i);
		}
		return sum;
	}

	private long getSampledEvents() {
		long sum = 0;
		for (int i = 0; i < sSlackManager.getSSSize(); i++) {
			sum += getSampledEvents(i);
		}
		return sum;
	}

	private long getSampledEvents(int localSSIndex) {
		return sampledEvents[localSSIndex];
	}

	private double getSamplingRate() {
		long observedEvents = getObservedEvents();
		long sampledEvents = getSampledEvents();
		return (sampledEvents * 1.0) / (observedEvents * 1.0);
	}

	public double getSamplingRate(int localSSIndex) {
		return (getSampledEvents(localSSIndex) * 1.0) / (getObservedEvents(localSSIndex) * 1.0);
	}

	public long getObservedEvents(int localSSIndex) {
		return sampledEvents[localSSIndex] + shedEvents[localSSIndex];
	}

	/* Metrics */
	Histogram getEventsPerSSHisto() {
		return eventsPerSSHisto;
	}

	Histogram getSamplingRatePerSSHisto() {
		return samplingRatePerSSHisto;
	}
}
