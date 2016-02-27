package optimizer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import event.Window;
import graph.*;

public class Exh_maxPartition extends Partitioner {	
	
	public Exh_maxPartition (ArrayDeque<Window> w) {
		super(w);
	}
	
	public Partitioning getPartitioning (Partitioning root, int part_num) {
		
		// Set local variables
		Partitioning solution = new Partitioning(new ArrayList<Partition>());
		
		double minCPU = Double.MAX_VALUE;
		ArrayList<Integer> memCosts = new ArrayList<Integer>();
		int maxHeapSize = 0;
		int considered_count = 0;
			
		LinkedList<Partitioning> heap = new LinkedList<Partitioning>();
		heap.add(root);		
				
		while (!heap.isEmpty()) {
			
			// Get the next node to process
			Partitioning temp = heap.poll();			
			double temp_cpu = temp.getCPUcost(windows);
			double temp_mem = temp.getMEMcost(windows);
			memCosts.add(new Double(temp_mem).intValue());
			considered_count++;
			
			// System.out.println("Considered: " + temp.toString(windows));
			
			// Update solution			
			if (minCPU > temp_cpu && temp.partitions.size() == part_num) { // && temp_mem <= memory_limit
				solution = temp;
				minCPU = temp_cpu;
			}			
			if (temp.partitions.size() > part_num) break;			
			
			// Add children to the heap			
			ArrayList<Partitioning> children = temp.getChildrenBySplitting();
			for (Partitioning child : children) {					
				if (!heap.contains(child)) heap.add(child); 
			} 
			// Update max heap size
			if (maxHeapSize < heap.size()) maxHeapSize = heap.size();			
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
		
		//System.out.println("Chosen: " + solution.toString(windows)); 
		
		return solution;		
	}
}
