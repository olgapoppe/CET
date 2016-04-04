package optimizer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import event.Event;
import event.Window;
import graph.*;

public class Exh_topDown extends Partitioner {	
	
	public Exh_topDown (ArrayDeque<Window> w) {
		super(w);
	}
	
	public Partitioning getPartitioning (ArrayList<Event> batch, double memory_limit) { // double part_num
		
		// Set local variables
		Partitioning max_partitioning = Partitioning.getPartitioningWithMaxPartition(batch);
		System.out.println("Input: " + max_partitioning.toString(2));
		Partitioning solution = new Partitioning(new ArrayList<Partition>());
		
		double minCPU = Double.MAX_VALUE;
		ArrayList<Integer> memCosts = new ArrayList<Integer>();
		int maxHeapSize = 0;
		int considered_count = 0;
		int algorithm = 2;
			
		LinkedList<Partitioning> heap = new LinkedList<Partitioning>();
		heap.add(max_partitioning);		
				
		while (!heap.isEmpty()) {
			
			// Get the next node to process
			Partitioning temp = heap.poll();			
			double temp_cpu = temp.getCPUcost(algorithm);
			double temp_mem = temp.getMEMcost(algorithm);
			memCosts.add(new Double(temp_mem).intValue());
			considered_count++;
			
			//System.out.println("Considered: " + temp.toString(algorithm));
			
			// Update solution			
			if (minCPU > temp_cpu && temp_mem <= memory_limit) { // temp.partitions.size() == part_num) {
				solution = temp;
				minCPU = temp_cpu;
			}			
			//if (temp.partitions.size() > part_num) break;			
			
			// Add children to the heap			
			ArrayList<Partitioning> children = temp.getChildrenBySplitting();
			for (Partitioning child : children) {					
				if (!heap.contains(child)) heap.add(child); 
			} 
			// Update max heap size
			if (maxHeapSize < heap.size()) maxHeapSize = heap.size();
			algorithm = 3;
		}
		// Compute median memory cost
		int length = memCosts.size();
		Integer[] array = new Integer [length];
		array = memCosts.toArray(array);
		Arrays.sort(array);
		int median = array[length/2];
				
		System.out.println("Max heap size: " + maxHeapSize + 
				"\nConsidered: " + considered_count +
				"\nMedian memory cost: " + median);
		
		Partitioning result = (solution.partitions.isEmpty()) ? max_partitioning : solution;
		algorithm = (solution.partitions.isEmpty()) ? 1 : 3;
		System.out.println("Chosen: " + result.toString(3));
		return result;		
	}
}
