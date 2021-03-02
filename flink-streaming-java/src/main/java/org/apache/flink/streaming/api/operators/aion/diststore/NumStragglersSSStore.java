package org.apache.flink.streaming.api.operators.aion.diststore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.PriorityQueue;

public class NumStragglersSSStore {

	protected static final Logger LOG = LoggerFactory.getLogger(NumStragglersSSStore.class);

	private final PriorityQueue<Integer> eventsQueue;
	private double totalStragglers;
	private int count;
	private int total;
	private long total_squared;
	private boolean isPurged;
	private int numSubStreams = 0;
	private int first_three = 3;


	public NumStragglersSSStore(int numSubStreams) {
		this.count = 0;
		this.isPurged = false;
		this.eventsQueue = new PriorityQueue<>();
		this.numSubStreams = numSubStreams;
		total = 0;
	}

	public void addValue(int numStragglers){
		if (first_three > 0){
			first_three--;
			return;
		}
		if(numStragglers < 6){
			return;
		}
		System.out.println("Adding value " + numStragglers);
		int head;
		int QUEUE_SIZE = 64;
		if (this.eventsQueue.size() > QUEUE_SIZE){
			head = eventsQueue.poll();
			total -= head;
			total_squared -= (int) Math.pow(head, 2);
			count -= 1;
		}
		eventsQueue.add(numStragglers);
		total += numStragglers;
		total_squared += (int) Math.pow(numStragglers, 2);
		count += 1;
	}

	public int getMean(){
		return total/count;
	}

	public double getSD(){
		if(count < 5) {
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
		if(count < 5) {
			return 0;
		}

		int mean = getMean();
		System.out.println("Mean: " + mean + " count: " + count + " sd: " + getSD());
		return (int) Math.max(Math.ceil(mean - this.getSD()), 0);
	}
}
