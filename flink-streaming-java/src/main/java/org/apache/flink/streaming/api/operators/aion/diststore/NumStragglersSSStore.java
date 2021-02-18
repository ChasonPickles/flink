package org.apache.flink.streaming.api.operators.aion.diststore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.PriorityQueue;

public class NumStragglersSSStore {

	protected static final Logger LOG = LoggerFactory.getLogger(NumStragglersSSStore.class);

	private final PriorityQueue<Integer> eventsQueue;
	private double totalStragglers;
	private double mean;
	private double sd;
	private int count;
	private int total;
	private boolean isPurged;
	private int numSubStreams = 0;
	private final int QUEUE_SIZE = 64;


	public NumStragglersSSStore(int numSubStreams) {
		this.mean = 0;
		this.sd = 0;
		this.count = 0;
		this.isPurged = false;
		this.eventsQueue = new PriorityQueue<>();
		this.numSubStreams = numSubStreams;
		total = 0;
	}

	public void addValue(int numStragglers){
		if(numStragglers < 5){
			return;
		}
		int head;
		if (this.eventsQueue.size() > QUEUE_SIZE){
			head = eventsQueue.poll();
			total -= head;
			count -= 1;
		}
		eventsQueue.add(numStragglers);
		total += numStragglers;
		count += 1;
	}

	public int getMean(){
		return total/count;
	}

	public double getSD(){
		if(count < 2) {
			return 0;
		}
		int mean = getMean();
		int temp_total_squared = 0;
		for(int i: eventsQueue){
			temp_total_squared +=  (i-mean)*(i-mean);
		}

		return Math.sqrt(temp_total_squared/(count-1.0));
	}

	public int getConservateEstimateForStragglers(){
		if(count < 2) {
			return 0;
		}

		int mean = getMean();
		System.out.println("Mean: " + mean + " count: " + count + " sd: " + getSD());
		return (int) Math.max(Math.ceil(mean - this.getSD()), 0);
	}
}
