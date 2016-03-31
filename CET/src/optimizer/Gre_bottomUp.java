package optimizer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import event.Window;

public class Gre_bottomUp { //extends Partitioner {	
	
	/*public Gre_bottomUp (ArrayDeque<Window> w) {
		super(w);
	}
	
	public Partitioning getPartitioning (Partitioning temp, double memory_limit) {
		
		// Set local variables
		Partitioning solution = temp;
		int algorithm = 1;
		double minCPU = temp.getCPUcost(windows,algorithm);
		int maxChildrenNumber = 0;
		//System.out.println("Best so far: " + solution.toString());
		
		while (temp!=null) {
			
			//System.out.println("Considered: " + temp.toString());
			
			// Consider all children, update their max number and pick the best of them
			ArrayList<Partitioning> children = temp.getChildrenByMerging();
			int number = children.size();
			if (maxChildrenNumber<number) maxChildrenNumber = number;
			
			temp = null;
			for (Partitioning child : children) {
				double child_cpu = child.getCPUcost(windows,algorithm);
				double child_mem = child.getMEMcost(windows,algorithm);
				if (minCPU > child_cpu && child_mem <= memory_limit) {
					minCPU = child_cpu;
					solution = child;
					temp = child;
					//System.out.println("Best so far: " + solution.toString());
				}
			}
			algorithm = 3;
		}
		System.out.println("Max children number: " + maxChildrenNumber);
		return solution;		
	}*/
}
