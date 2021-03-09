package org.apache.flink.streaming.api.operators.aion;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.operators.aion.diststore.DistStoreManager;
import org.apache.flink.streaming.api.operators.aion.estimators.WindowSizeEstimator;
import org.apache.flink.streaming.api.operators.aion.sampling.AbstractSSlackAlg;
import org.apache.flink.streaming.api.operators.aion.sampling.KSlackNoSampling;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedWriter;
import java.io.FileWriter;

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
	private final long windowLength; // in milliseconds
	private final long ssLength; // in milliseconds
	private final int numberSSPerWindow;
	/* Watermarks. */
	private long lastEmittedWatermark = Long.MIN_VALUE;
	private long lastEmittedWindowWatermark = Long.MIN_VALUE;
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
	protected BufferedWriter writer;
	public String workloadType;

	public WindowSSlackManager(
		final ProcessingTimeService processingTimeService,
		final long windowLength,
		final long ssLength,
		final int numberSSPerWindow,
		final String workloadType) {
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
		this.workloadType = workloadType;
		try {
			writer = new BufferedWriter(new FileWriter("late_events.txt", false));
			writer.write("uniqueId" + ",timestamp" + ",LastEmittedWatermark" +  ",windowEndTime\n");
		}catch(Exception e){
			e.printStackTrace();
		}

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

	/* returns 1 if new watermark changes the current active window
	* ie. if lastEmittedWindowWatermark changes, return 1. 0 otherwise */
	public int setLastEmittedWatermark(long targetWatermark) {
		if (targetWatermark > lastEmittedWatermark){
			lastEmittedWatermark = targetWatermark;

			long tmp = lastEmittedWatermark/windowLength;
			tmp = tmp*windowLength;
			if (lastEmittedWindowWatermark < tmp){
				lastEmittedWindowWatermark = tmp;
				return 1;
			}
		}
		return 0;
	}

	public long getLastEmittedWindowWatermark(){
		return lastEmittedWindowWatermark;
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

	public WindowSSlack getWindowFromIndex(long windowIndex) {
		return windowSlacksMap.get(windowIndex);
	}

	public long getSSDeadline(long windowIndex, long ssIndex) {
		return windowIndex * windowLength + (ssIndex + 1) * ssLength;
	}

	void recordWatermark(long watermark) {
		watDelays.update(System.currentTimeMillis() - watermark);
		watEmissionTimes.add(watermark);
	}

	void writeToOutput(String s){
		try {
			writer.write(s);
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	public void printStats() {
		if (this.isPrintingStats) {
			return;
		}
		BufferedWriter writer2;
		try {
			String slack_results_file = "slack_results.txt";
			writer2 = new BufferedWriter(new FileWriter(slack_results_file, false));
			this.isPrintingStats = true;
			StringBuilder sb = new StringBuilder();

			List<WindowSSlack> windows = new ArrayList<>(windowSlacksMap.values());
			windows.sort((left, right) -> (int) (left.getWindowIndex() - right.getWindowIndex()));

			ArrayList<WindowSSlack> results = new ArrayList<>();
			results.addAll(windows);
		/*
		for (WindowSSlack window : windows) {
			sb.append("Start Time:\t").append(window.startOfWindowTime).append("\n");
			sb.append("Events Processed\t").append(window.total_real_events).append("\n");
			sb.append("View Events\t").append(window.total_real_view_events).append("\n");
			sb.append("=============").append("\n");
		}
		 */
			sb.append("(window, total_real_view_events, results, straggler events)").append("\n");
			if(workloadType.equals("ysb")) {
				sb.append("workload type: ysb\n");
				sb.append(results.size()  + ": \n");
				for (WindowSSlack w : results) {
					sb
						.append("(")
						.append(w.getWindowIndex())
						.append(",")
						.append(w.total_real_view_events)
						.append(",")
						.append(w.expected_view_events)
						.append(",")
						.append(w.straggler_events)
						.append(")\n");
				}
			}else if (workloadType.equals("nyt")) {
				for (WindowSSlack w : results) {
					sb
						.append("(")
						.append(w.getWindowIndex())
						.append(",")
						.append(w.sum_only_real/w.total_real_events)
						.append(",")
						.append(w.sum/(w.total_real_events + w.total_fake_events))
						.append(",")
						.append(w.straggler_events)
						.append(")\n");
				}
			}
			writer2.write(sb.toString());
			writer.close();
			writer2.close();
		}catch(Exception e){
			e.printStackTrace();
		}
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
