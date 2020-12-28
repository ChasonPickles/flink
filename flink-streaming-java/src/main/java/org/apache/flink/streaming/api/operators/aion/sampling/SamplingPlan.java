package org.apache.flink.streaming.api.operators.aion.sampling;


import org.apache.flink.streaming.api.operators.aion.WindowSSlack;

class SamplingPlan {
	private final WindowSSlack windowSSlack;
	private final long[] ssEventsNum;
	private final double[] samplingRates;

	/* Purging */
	private int unpurgedSS;
	private final boolean[] isPurged;

	SamplingPlan(final WindowSSlack windowSSlack,
				 final int ssSize) {
		this.windowSSlack = windowSSlack;

		this.ssEventsNum = new long[ssSize];
		this.samplingRates = new double[ssSize];
		this.isPurged = new boolean[ssSize];

		this.unpurgedSS = ssSize;
	}

	public void updatePlan(int localSSIndex, long numOfEvents, double sr) {
		assert !isPurged[localSSIndex];

		ssEventsNum[localSSIndex] = numOfEvents;
		samplingRates[localSSIndex] = sr;
	}

	public int getNumOfUnpurgedSS() {
		return unpurgedSS;
	}

	public boolean isSSPurged(int localSSIndex) {
		return isPurged[localSSIndex];
	}

	public double getSamplingRate(int localSSIndex) {
		if (isPurged[localSSIndex]) {
			return windowSSlack.getSamplingRate(localSSIndex);
		}
		return samplingRates[localSSIndex];
	}

	public long getObservedEvents(int localSSIndex) {
		if (isPurged[localSSIndex]) {
			return windowSSlack.getObservedEvents(localSSIndex);
		}
		return ssEventsNum[localSSIndex];
	}

	void purgeSS(int localSSIndex) {
		for (int i = 0; i <= localSSIndex; i++) {
			if (!isPurged[i]) {
				isPurged[i] = true;
				unpurgedSS--;
			}
		}
	}
}
