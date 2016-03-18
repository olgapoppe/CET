package optimizer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedList;

import event.Window;
import graph.*;

public class BandB_minPartitions extends Partitioner {	
	
	public BandB_minPartitions (ArrayDeque<Window> w) {
		super(w);
	}
	
	public Partitioning getPartitioning (Partitioning root, int memory_limit, int bin_number, int bin_size) {
		
		// Set local variables
		Partitioning solution = new Partitioning(new ArrayList<Partition>());
		
		double minCPU = Integer.MAX_VALUE;
		int maxHeapSize = 0;
		int algorithm = 1;
		
		LinkedList<Partitioning> heap = new LinkedList<Partitioning>();
		heap.add(root);
		
		int pruning_1_count = 0;
		int pruning_2_count = 0;
		int considered_count = 0;
		int improved_count = 0;
		
		while (!heap.isEmpty()) {
			
			// Get current node and compute its costs 
			Partitioning temp = heap.poll();			
			double temp_cpu = temp.getCPUcost(windows,algorithm);
			considered_count++;
			
			//System.out.println("Considered: " + temp.toString());
			
			// Update the best solution seen so far
			if (minCPU > temp_cpu) {
				minCPU = temp_cpu;
				solution = temp;
				improved_count++;
				
				//System.out.println("Best so far: " + solution.toString());
			}
			// Add children to the heap and store their memory cost
			ArrayList<Partitioning> children = temp.getChildrenByMerging();
			for (Partitioning child : children) {
				double child_mem = child.getMEMcost(windows,algorithm);
				double child_cpu = child.getCPUcost(windows,algorithm);
				
				if  (child_mem > memory_limit) pruning_1_count++;
				if  (child_cpu > minCPU) pruning_2_count++;
				
				if (child_mem <= memory_limit && child_cpu <= minCPU && !heap.contains(child)) {
					heap.add(child);					
				}
			}
			// Update max heap size
			if (maxHeapSize < heap.size()) maxHeapSize = heap.size();
			algorithm = 3;
		}
		System.out.println("Max heap size: " + maxHeapSize + 
				"\nPruning 1: " + pruning_1_count + 
				"\nPruning 2: " + pruning_2_count + 
				"\nConsidered: " + considered_count +
				"\nImproved: " + improved_count);		
		return solution;		
	}
}
