package optimizer;

import java.util.ArrayList;
import java.util.LinkedList;
import graph.*;

public class BranchAndBound implements Partitioner {	
	
	public Partitioning getPartitioning (Partitioning root, int memory_limit) {
		
		// Set local variables
		Partitioning solution = new Partitioning(new ArrayList<Partition>());
		
		double minCPU = Integer.MAX_VALUE;
		int maxHeapSize = 0;
		
		LinkedList<Partitioning> heap = new LinkedList<Partitioning>();
		heap.add(root);
		
		while (!heap.isEmpty()) {
			
			// Get current node and compute its costs 
			Partitioning temp = heap.poll();			
			double temp_cpu = temp.getCPUcost();
			
			//System.out.println("Considered: " + temp.toString());
			
			// Update the best solution seen so far
			if (minCPU > temp_cpu) {
				minCPU = temp_cpu;
				solution = temp;
				
				//System.out.println("Best so far: " + solution.toString());
			}
			// Add children to the heap and store their memory cost
			ArrayList<Partitioning> children = temp.getChildren();
			for (Partitioning child : children) {
				double child_mem = child.getMEMcost();
				if (child_mem <= memory_limit && !heap.contains(child)) heap.add(child);
			}
			// Update max heap size
			if (maxHeapSize < heap.size()) maxHeapSize = heap.size();
		}
		System.out.println("Max heap size: " + maxHeapSize);		
		return solution;		
	}
}
