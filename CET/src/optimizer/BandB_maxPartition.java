package optimizer;

import java.util.ArrayList;
import java.util.LinkedList;
import graph.*;

public class BandB_maxPartition implements Partitioner {	
	
	public Partitioning getPartitioning (Partitioning root, int memory_limit) {
		
		// Set local variables
		ArrayList<Partitioning> solutions = new ArrayList<Partitioning>();
		Partitioning solution = new Partitioning(new ArrayList<Partition>());
		
		int maxHeapSize = 0;
		int considered_count = 0;
			
		LinkedList<Partitioning> heap = new LinkedList<Partitioning>();
		heap.add(root);		
				
		while (!heap.isEmpty()) {
			
			// Get the next node to process
			Partitioning temp = heap.poll();			
			if (temp.marked) continue;
			double temp_mem = temp.getMEMcost();
			considered_count++;
			
			ArrayList<Partitioning> children = temp.getChildrenBySplitting();
			
			// System.out.println("Considered: " + temp.toString());
			
			if (temp_mem > memory_limit) {
				
				// Add children to the heap				
				for (Partitioning child : children) {					
					if (!heap.contains(child)) heap.add(child); 
				} 
				// Update max heap size
				if (maxHeapSize < heap.size()) maxHeapSize = heap.size();
			} else {
				// Add this node to solutions and remember its children
				solutions.add(temp);
				for (Partitioning child : children) {					
					child.marked = true; 
				} 
			}
		}
		// Get solution with minimal CPU
		double minCPU = Double.MAX_VALUE;
		for (Partitioning p : solutions) {
			double p_cpu = p.getCPUcost();
			if (minCPU > p_cpu) {
				solution = p;
				minCPU = p_cpu;
			}
		}		
		System.out.println("Max heap size: " + maxHeapSize + 
				"\nConsidered: " + considered_count);		
		
		// System.out.println("Chosen: " + solution.toString()); 
		
		return solution;		
	}
}
