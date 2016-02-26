package optimizer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

import event.Window;
import graph.*;

public class BandB_maxPartition extends Partitioner {	
	
	public BandB_maxPartition (ArrayDeque<Window> w) {
		super(w);
	}
	
	public Partitioning getPartitioning (Partitioning root, int memory_limit) {
		
		// Set local variables
		Partitioning solution = new Partitioning(new ArrayList<Partition>());
		HashMap<String,Integer> pruned = new HashMap<String,Integer>();
		
		double minCPU = Double.MAX_VALUE;		
		int maxHeapSize = 0;
		int considered_count = 0;
			
		LinkedList<Partitioning> heap = new LinkedList<Partitioning>();
		heap.add(root);		
				
		while (!heap.isEmpty()) {
			
			// Get the next node to process, its costs and children 
			Partitioning temp = heap.poll();			
			if (pruned.containsKey(temp.id)) continue;
			double temp_cpu = temp.getCPUcost(windows);
			double temp_mem = temp.getMEMcost(windows);
			ArrayList<Partitioning> children = temp.getChildrenBySplitting();
			
			//System.out.println("Considered: " + temp.toString());
			
			considered_count++;
			
			if (temp_mem > memory_limit) {
				
				// Add children to the heap				
				for (Partitioning child : children) {					
					if (!heap.contains(child) && !pruned.containsKey(child.id)) 
						heap.add(child); 
				} 
				// Update max heap size
				if (maxHeapSize < heap.size()) 
					maxHeapSize = heap.size();
			} else {
				// Update solution
				if (temp_cpu < minCPU) {
					solution = temp;
					minCPU = temp_cpu;
				}
				// Prune the children
				for (Partitioning child : children) {
					pruned.put(child.id, 1);
				}
			}
		}
		System.out.println("Max heap size: " + maxHeapSize + 
				"\nConsidered: " + considered_count);		
		
		//System.out.println("Chosen: " + solution.toString()); 
		
		return solution;		
	}
}
