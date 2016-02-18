package optimizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import graph.*;

public class Exhaustive implements Partitioner {	
	
	public Partitioning getPartitioning (Partitioning root, int memory_limit) {
		
		Partitioning solution = new Partitioning(new ArrayList<Partition>());
		
		double minCPU = Integer.MAX_VALUE;
		double maxHeapSize = 0;
		int search_space_size = new Double(Math.pow(2, root.partitions.size())).intValue();
		double[] memCosts = new double[search_space_size];
		int i = 0;
		
		LinkedList<Partitioning> heap = new LinkedList<Partitioning>();
		heap.add(root);
		
		while (!heap.isEmpty()) {
			Partitioning temp = heap.poll();
			
			System.out.println("Considered: " + temp.toString());
			
			double temp_cpu = temp.getCPUcost();
			double temp_mem = temp.getMEMcost();
			if (minCPU > temp_cpu && temp_mem <= memory_limit) {
				minCPU = temp_cpu;
				solution = temp;
				
				System.out.println("Better: " + solution.toString());
			}
			ArrayList<Partitioning> children = temp.getChildren();
			for (Partitioning child : children) {
				double child_mem = child.getMEMcost();
				memCosts[i] = child_mem;
				i++;
				if (!heap.contains(child)) heap.add(child);
			}
			if (maxHeapSize < heap.size()) maxHeapSize = heap.size();
		}
		Arrays.sort(memCosts);
		double median = (double)memCosts[memCosts.length/2];
		System.out.println("Max heap size: " + maxHeapSize +
				"\nMedian memory cost: " + median);
		return solution;		
	}
}
