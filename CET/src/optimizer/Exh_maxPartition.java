package optimizer;

import java.util.ArrayList;
import java.util.LinkedList;
import graph.*;

public class Exh_maxPartition implements Partitioner {	
	
	public Partitioning getPartitioning (Partitioning root, int memory_limit) {
		
		// Set local variables
		Partitioning solution = new Partitioning(new ArrayList<Partition>());
		
		double minCPU = Double.MAX_VALUE;
		int maxHeapSize = 0;
		int considered_count = 0;
			
		LinkedList<Partitioning> heap = new LinkedList<Partitioning>();
		heap.add(root);		
				
		while (!heap.isEmpty()) {
			
			// Get the next node to process
			Partitioning temp = heap.poll();			
			double temp_cpu = temp.getCPUcost();
			double temp_mem = temp.getMEMcost();
			considered_count++;
			
			// System.out.println("Considered: " + temp.toString());
			
			// Update solution			
			if (minCPU > temp_cpu && temp_mem <= memory_limit) {
				solution = temp;
				minCPU = temp_cpu;
			}			
			// Add children to the heap			
			ArrayList<Partitioning> children = temp.getChildrenBySplitting();
			for (Partitioning child : children) {					
				if (!heap.contains(child)) heap.add(child); 
			} 
			// Update max heap size
			if (maxHeapSize < heap.size()) maxHeapSize = heap.size();
			
		}
		System.out.println("Max heap size: " + maxHeapSize + 
				"\nConsidered: " + considered_count);		
		
		// System.out.println("Chosen: " + solution.toString()); 
		
		return solution;		
	}
}
