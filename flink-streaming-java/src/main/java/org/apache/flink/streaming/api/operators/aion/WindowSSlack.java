package org.apache.flink.streaming.api.operators.aion;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.operators.aion.diststore.WindowDistStore;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.streaming.api.operators.aion.WindowSSlackManager.MAX_NET_DELAY;
import static org.apache.flink.streaming.api.operators.aion.WindowSSlackManager.STATS_SIZE;

public class WindowSSlack {

	protected static final Logger LOG = LoggerFactory.getLogger(WindowSSlack.class);

	/* Tracking real value of Window */
	public long total_real_view_events;
	public long total_real_events;
	public long total_fake_events;
	public long fake_events_stragglers;
	public int straggler_events;
	public int expected_view_events;

	public double sum;
	public double sum_only_real;

	/* Identifiers for WindowSS */
	private final long windowIndex;
	private final WindowSSlackManager sSlackManager;
	public final long startOfWindowTime;
	public final long windowEndTime;

	/* Stores */
	private final WindowDistStore netDelayStore;
	private final WindowDistStore genDelayStore;

	private final long[] sampledEvents;
	private final long[] shedEvents;

	/* Metrics */
	private final Histogram eventsPerSSHisto;
	private final Histogram samplingRatePerSSHisto;

	//BufferedWriter writer;

	WindowSSlack(
			/* Identifiers */
			final WindowSSlackManager sSlackManager,
			final long windowIndex,
			final long eventTime,
			final long windowSize) {
		this.windowIndex = windowIndex;
		this.sSlackManager = sSlackManager;

		this.netDelayStore = sSlackManager.getNetDelayStoreManager().createWindowDistStore(this);
		this.genDelayStore = sSlackManager.getInterEventStoreManager().createWindowDistStore(this);

		this.sampledEvents = new long[sSlackManager.getNumberOfSSPerWindow()];
		this.shedEvents = new long[sSlackManager.getNumberOfSSPerWindow()];

		startOfWindowTime = TimeWindow.getWindowStartWithOffset(eventTime, 0, windowSize);
		windowEndTime = startOfWindowTime + windowSize;

		this.eventsPerSSHisto = new DescriptiveStatisticsHistogram(STATS_SIZE);
		this.samplingRatePerSSHisto = new DescriptiveStatisticsHistogram(STATS_SIZE);
		straggler_events = 0;
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
		}else{
			shedEvents[localSSIndex]++;
			return false;
		}
	}

	/*
	 * Public interface that determines to emit watermark or not.
	 *
	 * @returns a boolean value that determines if the tuple to be included in the sample.
	 */
	public long emitWatermark(long timestamp) {
		long n = (timestamp - startOfWindowTime)/sSlackManager.getSSLength();
		assert startOfWindowTime + n*sSlackManager.getSSLength() <= timestamp;
		return startOfWindowTime + n*sSlackManager.getSSLength();
		/*
		if (timestamp > startOfWindowTime) {
			return startOfWindowTime;
		}
		else {
			return -1;
		}
		 */
		//long watTime = sSlackManager.getsSlackAlg().emitWatermark();
		//if (watTime != -1) {
		//	sSlackManager.recordWatermark(watTime);
		//}
		//return watTime;
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

	public long getWindowDeadline() {
		return windowEndTime;
	}

	/* Manipulation functions for book-keept data */
	public boolean isPurged(int localSSIndex) {
		return netDelayStore.isPurged(localSSIndex);
	}

	private long getSampledEvents(int localSSIndex) {
		return sampledEvents[localSSIndex];
	}

	public double getSamplingRate(int localSSIndex) {
		return (getSampledEvents(localSSIndex) * 1.0) / (getObservedEvents(localSSIndex) * 1.0);
	}

	public long getObservedEvents(int localSSIndex) {
		return sampledEvents[localSSIndex] + shedEvents[localSSIndex];
	}

	public void processEventYSB(JSONObject jsonEvent, long timestamp) {
		if (!jsonEvent.has("fake")){
			total_real_events += 1;
			if (jsonEvent.get("event_type").equals("view")){
				total_real_view_events += 1;
			}
			if(timestamp < sSlackManager.getLastEmittedWindowWatermark()){
				straggler_events++;
				String uniqueId = jsonEvent.getString("uniqueId");
				if (uniqueId != null) {
					String s = "late," + uniqueId + "," + timestamp + "," + sSlackManager.getLastEmittedWatermark()
						+ "," + sSlackManager.getLastEmittedWindowWatermark() + "," + (windowEndTime - 1) + "\n";
					sSlackManager.writeToOutput(s);
				}
			}else{
				String uniqueId = jsonEvent.getString("uniqueId");
				if (uniqueId != null) {
					String s = "time," + uniqueId + "," + timestamp + "," + sSlackManager.getLastEmittedWatermark() +
						"," + sSlackManager.getLastEmittedWindowWatermark() + ", " + (windowEndTime - 1) + "\n";
					sSlackManager.writeToOutput(s);
				}
				if (jsonEvent.get("event_type").equals("view")) {
					expected_view_events += 1;
				}
			}
		}
	}

	public void processEventNYT(JSONObject jsonEvent, long timestamp) {
		double fare_amount = Double.parseDouble(jsonEvent.getString("fare_amount"));
		if (!jsonEvent.has("fake")){
			total_real_events += 1;
			sum_only_real += fare_amount;
			if(timestamp < sSlackManager.getLastEmittedWindowWatermark()){
				straggler_events++;
			}
		}else{
			total_fake_events += 1;
		}
		sum += fare_amount;
	}

	/* Metrics */
	Histogram getEventsPerSSHisto() {
		return eventsPerSSHisto;
	}

	Histogram getSamplingRatePerSSHisto() {
		return samplingRatePerSSHisto;
	}
}
