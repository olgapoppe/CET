package optimizer;

import java.util.ArrayList;
import java.util.LinkedList;

import graph.*;

public class BranchAndBound implements Partitioner {	
	
	public Partitioning getPartitioning (Partitioning root, int memory_limit) {
		
		Partitioning solution = new Partitioning(new ArrayList<Partition>());
		
		double minCPU = Integer.MAX_VALUE;
		
		LinkedList<Partitioning> heap = new LinkedList<Partitioning>();
		heap.add(root);
		
		while (!heap.isEmpty()) {
			Partitioning temp = heap.poll();
			
			System.out.println("Considered: " + temp.toString());
			
			double temp_cpu = temp.getCPUcost();
			if (minCPU > temp_cpu) {
				minCPU = temp_cpu;
				solution = temp;
				
				System.out.println("Better: " + solution.toString());
			}
			ArrayList<Partitioning> children = temp.getChildren();
			for (Partitioning child : children) {
				if (child.getMEMcost() <= memory_limit && !heap.contains(child)) 
					heap.add(child);
			}
		}		
		return solution;		
	}
}
