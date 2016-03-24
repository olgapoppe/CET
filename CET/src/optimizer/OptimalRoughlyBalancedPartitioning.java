package optimizer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import event.Window;
import graph.Partition;

public class OptimalRoughlyBalancedPartitioning extends Partitioner {
	
	public OptimalRoughlyBalancedPartitioning (ArrayDeque<Window> w) {
		super(w);
	}
		
	public Partitioning getPartitioning (Partitioning min_partitioning, double memory_limit, int bin_number, int bin_size) {
		
		// Set local variables
		Partitioning solution = new Partitioning(new ArrayList<Partition>());
		double minCPU = Double.MAX_VALUE;		
		int maxHeapSize = 0;
		int considered_count = 0;
							
		LinkedList<Partitioning> heap = new LinkedList<Partitioning>();
								
		while (!heap.isEmpty()) {
					
			// Get the next node to process 
			Partitioning temp = heap.poll();
			//System.out.println("Considered: " + temp.toString());
			considered_count++;
			
			// Update solution
			double temp_cpu = temp.getCPUcost(windows, 3);
			double temp_mem = temp.getMEMcost(windows, 3);
			if (temp_mem <= memory_limit && temp_cpu < minCPU) {
				solution = temp;
				minCPU = temp_cpu;
			}
			// Add children to the heap		
			/*ArrayList<Partitioning> children = temp.getChildren(bin_number, bin_size);
			for (Partitioning child : children) {					
				if (!heap.contains(child)) heap.add(child); 
			}*/			
			// Update max heap size
			if (maxHeapSize < heap.size()) maxHeapSize = heap.size();
		}		
		System.out.println("Max heap size: " + maxHeapSize + 
						"\nConsidered: " + considered_count);	
				
		//System.out.println("Chosen: " + solution.toString()); 
		return solution;
	}
}
