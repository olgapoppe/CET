package optimizer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

import event.*;
import graph.*;

public class BnB_topDown extends Partitioner {	
	
	public BnB_topDown (ArrayDeque<Window> w) {
		super(w);
	}
	
	public Partitioning getPartitioning (ArrayList<Event> batch, double memory_limit) {
		
		// Set local variables
		LinkedList<CutSet> heap = new LinkedList<CutSet>();
		CutSet bestcutset = new CutSet(new ArrayList<Integer>());
		HashMap<Integer,Integer> pruned = new HashMap<Integer,Integer>();
				
		double minCPU = Double.MAX_VALUE;		
		int maxHeapSize = 0;
		int considered_count = 0;
		
		/*** Level search ***/		
		// Get number of necessary cuts
		int level = getMinNumberOfRequiredPartitions_walkDown(batch,memory_limit);
		System.out.println("Min number of necessary cuts: " + level);
		
		/*** Node search ***/
		// Get the graph and its events per second 
		Partitioning max_partitioning = Partitioning.getPartitioningWithMaxPartition(batch);
		Partition max_partition = max_partitioning.partitions.get(0);
		HashMap<Integer,ArrayList<Node>> events_per_second = max_partition.events_per_second;
		int start = max_partition.start;
		int end = max_partition.end;
		
		// Get all possibilities to cut, cut the graph and store the nodes in the heap
		ArrayList<CutSet> cutsets = max_partition.getAllNearlyBalncedNotPrunedCutSets(level, pruned, events_per_second);
		heap.addAll(cutsets);
		System.out.println("There are " + cutsets.size() + " nearly balanced not pruned nodes at level " + level + "\n");		
		
		// Update max heap size
		if (maxHeapSize < heap.size()) maxHeapSize = heap.size();
				
		// Find optimal solution
		while (!heap.isEmpty()) {
			
			// Get the next node to process, its costs and children 
			CutSet temp = heap.poll();			
			if (temp.isPruned(pruned)) continue;			
			double temp_cpu = temp.getCPUcost(start,end,events_per_second);
			double temp_mem = temp.getMEMcost(start,end,events_per_second);		
			//System.out.println("Considered: " + temp.toString(3));
			considered_count++;
			
			// Update the solution and prune the descendants
			if (temp_mem < memory_limit) {
				if (temp_cpu < minCPU) {
					bestcutset = temp;
					minCPU = temp_cpu;
				}
				for (Integer cut : temp.cutset) 
					pruned.put(cut, 1);				
			}		
			
			// Put all nearly balanced not pruned nodes from the next level to the heap
			if (temp.cutset.size() == level) {
				level++;
				cutsets = max_partition.getAllNearlyBalncedNotPrunedCutSets(level, pruned, events_per_second);
				heap.addAll(cutsets);
				System.out.println("There are " + cutsets.size() + " nearly balanced not pruned nodes at level " + level + "\n");
				
				// Update max heap size
				if (maxHeapSize < heap.size()) maxHeapSize = heap.size();
			}			
		}
		System.out.println("Max heap size: " + maxHeapSize + 
				"\nConsidered: " + considered_count);
		
		//System.out.println("Chosen: " + solution.toString()); 
		
		
		// Only the solution partitioning is actually constructed		
		return max_partition.getPartitioning(bestcutset);		
	}
	
	/*** Get minimal number of required partitions walking the search space top down ***/
	public int getMinNumberOfRequiredPartitions_walkDown(ArrayList<Event> batch, double memory_limit) {	
		
		int event_number = batch.size();
		
		// Find the number of minimal partitions
		int s = 1;
		int e = -1;
		int curr_sec = -1;		
		for(Event event : batch) {
			if (curr_sec < event.sec) {
				curr_sec = event.sec;
				e++;
		}}
		
		// Find the minimal number of required partitions
		int m = 0;
		double ideal_memory = 0;
		int level = 0;
		while (s <= e) {	
			m = s + (e-s)/2;
			ideal_memory = getIdealMEMcost(event_number,m+1,3);						
			System.out.println("m=" + m + " mem=" + ideal_memory);
			
			if (ideal_memory <= memory_limit) {
				level = m;
				e = m - 1;				
			} else {
				s = m + 1;
			}
			System.out.println("s=" + s + " e=" + e + "\n");
		}	
		return (level > 0) ? level : event_number;
	}
	
	public double getIdealMEMcost (int event_number, int partition_number, int algorithm) {
		
		double exp;
		double ideal_memory;
		
		if (algorithm == 1) {
			ideal_memory = event_number;
		} else {
		if (algorithm == 2) {
			ideal_memory = Math.pow(3, event_number) * event_number;			
		} else {
			double vertex_number_per_partition = event_number/new Double(partition_number);
			exp = vertex_number_per_partition/new Double(3);			
			ideal_memory = partition_number * Math.pow(3, exp) * vertex_number_per_partition + event_number;
		}}
		return ideal_memory;
	}
}
