package org.apache.flink.streaming.api.operators.aion;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.operators.aion.diststore.DistStoreManager;
import org.apache.flink.streaming.api.operators.aion.estimators.WindowSizeEstimator;
import org.apache.flink.streaming.api.operators.aion.sampling.AbstractSSlackAlg;
import org.apache.flink.streaming.api.operators.aion.sampling.KSlackNoSampling;
import org.apache.flink.streaming.api.operators.aion.sampling.NaiveSSlackAlg;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import static org.apache.flink.streaming.api.operators.aion.diststore.DistStoreManager.DistStoreType.GEN_DELAY;
import static org.apache.flink.streaming.api.operators.aion.diststore.DistStoreManager.DistStoreType.NET_DELAY;


/**
 * This class provides an interface for Source operators to retrieve windows.
 * Internally, this class manages all windows.
 */
public final class WindowSSlackManager {

	protected static final Logger LOG =
		LoggerFactory.getLogger(WindowSSlackManager.class);

	static final int MAX_NET_DELAY = 500; // We can tolerate up to 500ms max delay.
	private static final int HISTORY_SIZE = 1024;
	public static final int STATS_SIZE = 10000;

	private final ProcessingTimeService processingTimeService;
	private final AbstractSSlackAlg sSlackAlg;
	/* Logical division of windows */
	private final long windowLength;
	private final long ssLength;
	private final int numberSSPerWindow;
	/* Watermarks. */
	private long lastEmittedWatermark = Long.MIN_VALUE;
	/* Structures to maintain distributions & diststore. */
	private final DistStoreManager netDelayStoreManager;
	private final DistStoreManager interEventStoreManager;

	private final Map<Long, WindowSSlack> windowSlacksMap;
	/* Metrics */
	private final Counter windowsCounter;
	private final PriorityQueue<Long> watEmissionTimes;
	private final Histogram watDelays;
	private boolean isPrintingStats;
	/* Stats purger */
	//private final Thread timestampsPurger;
	private boolean isWarmedUp;

	public WindowSSlackManager(
		final ProcessingTimeService processingTimeService,
		final long windowLength,
		final long ssLength,
		final int numberSSPerWindow) {
		this.processingTimeService = processingTimeService;

		this.windowLength = windowLength;
		this.ssLength = ssLength;
		this.numberSSPerWindow = numberSSPerWindow;

		this.netDelayStoreManager = new DistStoreManager(this, NET_DELAY);
		this.interEventStoreManager = new DistStoreManager(this, GEN_DELAY);

		WindowSizeEstimator srEstimator =
			new WindowSizeEstimator(this, netDelayStoreManager, interEventStoreManager);
		this.sSlackAlg = new KSlackNoSampling(this, srEstimator);
		//this.sSlackAlg = new NaiveSSlackAlg(this, srEstimator);

		this.windowSlacksMap = new HashMap<>();

		/* Purging */
		this.isWarmedUp = false;
		//this.timestampsPurger = new Thread(new SSStatsPurger(processingTimeService.getCurrentProcessingTime()));
		//this.timestampsPurger.start();
		/* Metrics */
		this.windowsCounter = new SimpleCounter();
		this.watEmissionTimes = new PriorityQueue<>();
		this.watDelays = new DescriptiveStatisticsHistogram(STATS_SIZE);
		this.isPrintingStats = false;
	}

	/* Getters & Setters */
	long getWindowLength() {
		return windowLength;
	}

	public long getSSLength() {
		return ssLength;
	}

	public int getNumberOfSSPerWindow() {
		return numberSSPerWindow;
	}

	public ProcessingTimeService getProcessingTimeService() {
		return processingTimeService;
	}

	public boolean isWarmedUp() {
		return isWarmedUp;
	}

	public long getLastEmittedWatermark() {
		return lastEmittedWatermark;
	}

	public void setLastEmittedWatermark(long targetWatermark) {
		lastEmittedWatermark = targetWatermark;
	}

	AbstractSSlackAlg getsSlackAlg() {
		return sSlackAlg;
	}

	DistStoreManager getNetDelayStoreManager() {
		return netDelayStoreManager;
	}

	DistStoreManager getInterEventStoreManager() {
		return interEventStoreManager;
	}

	/* Map manipulation */
	public WindowSSlack getWindowSlack(long eventTime) {
		long windowIndex = getWindowIndex(eventTime);
		WindowSSlack ws = windowSlacksMap.getOrDefault(windowIndex, null);
		// New window!
		if (ws == null) {
			ws = new WindowSSlack(
				this,
				windowIndex,
				eventTime,
				windowLength);
			windowSlacksMap.put(windowIndex, ws);
			sSlackAlg.initWindow(ws);
			// Remove from history
			removeWindowSSlack(windowIndex - HISTORY_SIZE);
			windowsCounter.inc();
		}
		return ws;
	}

	private void removeWindowSSlack(long windowIndex) {
		WindowSSlack window = windowSlacksMap.remove(windowIndex);
		if (window != null) {
			LOG.info("Removing window slack {}", windowIndex);
		}
	}

	/* Index & Deadlines calculation */
	final long getWindowIndex(long eventTime) {
		return (long) Math.floor(eventTime / (windowLength * 1.0));
	}

	public long getWindowDeadline(long windowIndex) {
		 WindowSSlack ws =  windowSlacksMap.get(windowIndex);
		 return ws.getWindowDeadline();
	}

	public long getSSDeadline(long windowIndex, long ssIndex) {
		return windowIndex * windowLength + (ssIndex + 1) * ssLength;
	}

	void recordWatermark(long watermark) {
		watDelays.update(System.currentTimeMillis() - watermark);
		watEmissionTimes.add(watermark);
	}

	public void printStats() {
		if (this.isPrintingStats) {
			return;
		}

		this.isPrintingStats = true;
		StringBuilder sb = new StringBuilder();
		// This is gonna be a length function.
		sb.append("Number of Windows observed:\t").append(windowsCounter.getCount()).append("\n");
		sb.append("===\n");

		List<WindowSSlack> windows = new ArrayList<>(windowSlacksMap.values());
		windows.sort((left, right) -> (int) (left.getWindowIndex() - right.getWindowIndex()));

		for (WindowSSlack window : windows) {
			HistogramStatistics numOfEvents = window.getEventsPerSSHisto().getStatistics();
			HistogramStatistics sr = window.getSamplingRatePerSSHisto().getStatistics();

			sb.append("Window:\t").append(window.getWindowIndex()).append("\n");
			sb.append("Number of Events per SS:\t")
				.append(numOfEvents.size()).append("\t")
				.append(numOfEvents.getMean()).append("\t")
				.append(numOfEvents.getStdDev()).append("\n");
			sb.append("Sampling Rate per SS:\t")
				.append(sr.size()).append("\t")
				.append(sr.getMean()).append("\t")
				.append(sr.getStdDev()).append("\n");
			sb.append("===\n");
		}
		System.out.println(sb.toString());

		sb = new StringBuilder();

		// Algorithm
		HistogramStatistics algSizeError = getsSlackAlg().getSizeEstimationStatistics();
		HistogramStatistics algSRError = getsSlackAlg().getSREstimationStatistics();
		Histogram histogram = new DescriptiveStatisticsHistogram(STATS_SIZE);
		if (!watEmissionTimes.isEmpty()) {
			long timestamp = watEmissionTimes.poll();
			while (!watEmissionTimes.isEmpty()) {
				histogram.update(watEmissionTimes.peek() - timestamp);
				timestamp = watEmissionTimes.poll();
			}
		}
		HistogramStatistics algWatFreq = histogram.getStatistics();
		HistogramStatistics algDelays = watDelays.getStatistics();
		sb.append("Algorithm Stats:\n");
		sb.append("Size Error Estimation:\t")
			.append(algSizeError.size()).append("\t")
			.append(algSizeError.getMean()).append("\t")
			.append(algSizeError.getStdDev()).append("\n");
		sb.append("Sampling-Rate Error Estimation:\t")
			.append(algSRError.size()).append("\t")
			.append(algSRError.getMean()).append("\t")
			.append(algSRError.getStdDev()).append("\n");
		sb.append("Watermark Frequency:\t")
			.append(algWatFreq.size()).append("\t")
			.append(algWatFreq.getMean()).append("\t")
			.append(algWatFreq.getStdDev()).append("\n");
		sb.append("Watermark Delays:\t")
			.append(algDelays.size()).append("\t")
			.append(algDelays.getMean()).append("\t")
			.append(algDelays.getStdDev()).append("\n");
		sb.append("===\n");
		// Network & Inter-Event Gen Delays
		HistogramStatistics netDelay = netDelayStoreManager.getMeanDelay();
		HistogramStatistics interEventDelay = interEventStoreManager.getMeanDelay();
		sb.append("Delays:\n");
		sb.append("Net Delay:\t")
			.append(netDelay.size()).append("\t")
			.append(netDelay.getMean()).append("\t")
			.append(netDelay.getStdDev()).append("\n");
		sb.append("Inter-Event Generation Delay:\t")
			.append(interEventDelay.size()).append("\t")
			.append(interEventDelay.getMean()).append("\t")
			.append(interEventDelay.getStdDev()).append("\n");
		sb.append("===\n");
		System.out.println(sb.toString());
	}


	/* Purging Runnable that purges substreams stats */
	private class SSStatsPurger implements Runnable {

		private long currTime;
		private long ssUntilPurges;

		SSStatsPurger(long currTime) {
			this.currTime = currTime;
			this.ssUntilPurges = HISTORY_SIZE / 16;
		}

		@Override
		public void run() {
			sleep(10 * MAX_NET_DELAY); // essential

			while (true) {
				long windowIndex = getWindowIndex(currTime);
				// TODO(oibfarhat): Consider making this more efficient
				for (long currIndex = windowIndex - 15; currIndex <= windowIndex; currIndex++) {
					WindowSSlack ws = windowSlacksMap.getOrDefault(windowIndex, null);
					if (ws != null) {
						// Purging is used to set the next algorithm
						boolean purge = ws.purgeSS(currTime);
						if (purge && !isWarmedUp) {
							if (--this.ssUntilPurges == 0) {
								LOG.info("It is finally warmed up at t = {}", currTime);
								isWarmedUp = true;
							}
						}

					}
				}

				sleep(MAX_NET_DELAY);
				currTime += MAX_NET_DELAY;
			}
		}

		public void sleep(int delay) {
			try {
				Thread.sleep(delay);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
