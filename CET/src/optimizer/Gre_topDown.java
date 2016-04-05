package optimizer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import event.*;
import graph.*;

public class Gre_topDown extends Partitioner {	
	
	public Gre_topDown (ArrayDeque<Window> w) {
		super(w);
	}
	
	public Partitioning getPartitioning (ArrayList<Event> batch, double memory_limit) {
		
		// Set local variables
		LinkedList<CutSet> heap = new LinkedList<CutSet>();
						
		int maxHeapSize = 0;
		int considered_count = 0;
		
		/*** Level search ***/		
		// Get number of necessary cuts
		int level = getMinNumberOfRequiredPartitions_walkDown(batch,memory_limit);
				
		// Get the graph and its events per second 
		Partitioning max_partitioning = Partitioning.getPartitioningWithMaxPartition(batch);
		Partition max_partition = max_partitioning.partitions.get(0);
		int vertex_number = max_partition.vertexNumber;
		if (level == vertex_number) {
			System.out.println("Chosen: " + max_partitioning.toString(1));
			return max_partitioning;
		}
		
		/*** Node search ***/
		// Get all not pruned cut sets, construct nearly balanced partitionings, store them in the respective cut sets and store these cut sets in the heap
		ArrayList<CutSet> cutsets = max_partition.getAllCutSets(level);
		int ideal_partition_size = vertex_number / (level+1);
		System.out.println("Min number of necessary cuts is " + level + " of size " + ideal_partition_size);
		int count = 0;
		for (CutSet cutset : cutsets) {			
			Partitioning p = max_partition.getNearlyBalancedPartitioning(cutset, ideal_partition_size);
			if (p != null) {
				cutset.partitioning = p;
				heap.add(cutset);
				count++;
				break;
				//System.out.println(cutset.toString());	
		}}	
		//System.out.println("There are " + count + " nearly balanced not pruned nodes at level " + level + "\n");		
		
		// Update max heap size
		if (maxHeapSize < heap.size()) maxHeapSize = heap.size();
				
		// Find optimal solution
		while (!heap.isEmpty()) {
			
			// Get the next node to process, its costs and children 
			CutSet temp = heap.poll();			
			double temp_mem = temp.partitioning.getMEMcost(3);		
			//System.out.println("Considered: " + temp.toString() + " " + temp.partitioning.toString(3));
			considered_count++;
			
			// Return the solution
			if (temp_mem < memory_limit) {
				System.out.println("Chosen: " + temp.partitioning.toString(3) +
									"\nMax heap size: " + maxHeapSize + 
									"\nConsidered: " + considered_count);
				return temp.partitioning;		
			}			
			// Put all nearly balanced not pruned nodes from the next level to the heap
			if (temp.cutset.size() == level) {
				level++;
				cutsets = max_partition.getAllCutSets(level);				
				ideal_partition_size = vertex_number / (level+1);
				count = 0;
				for (CutSet cutset : cutsets) {			
					Partitioning p = max_partition.getNearlyBalancedPartitioning(cutset, ideal_partition_size);
					if (p != null) {
						cutset.partitioning = p;
						heap.add(cutset);
						count++;
						break;
						//System.out.println(cutset.toString());
				}}	
				//System.out.println("There are " + count + " nearly balanced not pruned nodes at level " + level + "\n");			
				
				// Update max heap size
				if (maxHeapSize < heap.size()) maxHeapSize = heap.size();
			}			
		}
		System.out.println("Max heap size: " + maxHeapSize + 
				"\nConsidered: " + considered_count);
		
		System.out.println("Chosen: " + max_partitioning.toString(1));						
		return max_partitioning;		
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
