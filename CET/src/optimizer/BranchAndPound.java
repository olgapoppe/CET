package optimizer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import graph.*;

public class BranchAndPound implements Partitioner {	
	
	public Partitioning getPartitioning (Partitioning root, int m) {
		
		Partitioning solution = new Partitioning(new ArrayList<Partition>());
		
		double minCPU = Integer.MAX_VALUE;
		
		ArrayDeque<Partitioning> heap = new ArrayDeque<Partitioning>();
		heap.add(root);
		
		while (!heap.isEmpty()) {
			Partitioning temp = heap.poll();
			double temp_cpu = temp.getCPUcost();
			if (minCPU > temp_cpu) {
				minCPU = temp_cpu;
				solution = temp;
			}
			ArrayList<Partitioning> children = temp.getChildren();
			for (Partitioning child : children) {
				if (child.getMEMcost() <= m) heap.push(child);
			}
		}		
		return solution;		
	}
}
