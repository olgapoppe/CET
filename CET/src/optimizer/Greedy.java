package optimizer;

import java.util.ArrayList;

public class Greedy implements Partitioner {	
	
	public Partitioning getPartitioning (Partitioning temp, int memory_limit) {
		
		// Set local variables
		Partitioning solution = temp;
		double minCPU = temp.getCPUcost();
		int maxChildrenNumber = 0;
		System.out.println("Best so far: " + solution.toString());
		
		while (temp!=null) {
			
			System.out.println("Considered: " + temp.toString());
			
			// Consider all children, update their max number and pick the best of them
			ArrayList<Partitioning> children = temp.getChildren();
			int number = children.size();
			if (maxChildrenNumber<number) maxChildrenNumber = number;
			
			temp = null;
			for (Partitioning child : children) {
				double child_cpu = child.getCPUcost();
				double child_mem = child.getMEMcost();
				if (minCPU > child_cpu && child_mem <= memory_limit) {
					minCPU = child_cpu;
					solution = child;
					temp = child;
					System.out.println("Best so far: " + solution.toString());
				}
			}			
		}
		System.out.println("Max children number: " + maxChildrenNumber);
		return solution;		
	}
}
