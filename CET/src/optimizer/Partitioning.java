package optimizer;

import java.util.ArrayList;
import java.util.LinkedList;
import event.*;
import graph.*;

public class Partitioning {
	
	String id;
	public ArrayList<Partition> partitions;	
		
	public Partitioning (ArrayList<Partition> p) {
		partitions = p;
		for (Partition part : p) {
			id += part.id + ";";
		}
	}
	
	public boolean equals (Object o) {
		Partitioning other = (Partitioning) o;
		if (this.partitions.size() != other.partitions.size()) return false;
		for (Partition part1 : this.partitions) {
			boolean pairFound = false;
			for (Partition part2 : other.partitions) {
				if (part1.equals(part2)) {
					pairFound = true;
					break;
				}
			}
			if (!pairFound) return false;
		}	
		return true;
	}
	
	/*** From the given batch of events construct a partitioning with one maximal partition ***/
	public static Partitioning getPartitioningWithMaxPartition (ArrayList<Event> events) {	
		
		// Generate a single partition from all events 
		Graph graph = Graph.constructGraph(events);
		int first_sec = events.get(0).sec;
		int last_sec = events.get(events.size()-1).sec;
		Partition part = new Partition(first_sec,last_sec,events.size(),graph.edgeNumber,graph.first_nodes,graph.last_nodes);
		part.events_per_second = graph.events_per_second;
		part.minPartitionNumber = graph.minPartitionNumber;
		
		// Return an partitioning with this partition
		ArrayList<Partition> parts = new ArrayList<Partition>();
		parts.add(part);
		Partitioning rootPartitioning = new Partitioning(parts);
		return rootPartitioning;		
	}
	
	/*** From the given batch of events construct a partitioning with minimal partitions ***/
	public static Partitioning getPartitioningWithMinPartitions (ArrayList<Event> events) {	
		
		int curr_sec = events.get(0).sec;
		ArrayList<Partition> parts = new ArrayList<Partition>();
		
		ArrayList<Event> batch = new ArrayList<Event>();
		for (Event event : events) {
			// Get all events with the same time stamp
			if (event.sec == curr_sec) {
				batch.add(event);
			} else {
				// Construct a partition from events with the same time stamp
				if (!batch.isEmpty()) {
					Partition minimalPartitioning = Partition.getMinPartition(curr_sec, batch);					
					parts.add(minimalPartitioning);
				}
				// Reset the current second and the batch for the next iteration
				curr_sec = event.sec;
				batch.clear();
				batch.add(event);
		}}
		// Last partition
		if (!batch.isEmpty()) {
			Partition minimalPartitioning = Partition.getMinPartition(curr_sec, batch);		
			parts.add(minimalPartitioning);
		}
		Partitioning rootPartitioning = new Partitioning(parts);
		System.out.println(rootPartitioning.partitions.size() + " minimal partitions.");
		return rootPartitioning;			
	}
	
	/*** Find an optimal partitioning at a level ***/
	public static Partitioning getOptimalPartitioning (ArrayList<Event> batch, int cut_number) {	
		
		// Set local variables
		LinkedList<CutSet> heap = new LinkedList<CutSet>();
		CutSet bestcutset = new CutSet(new ArrayList<Integer>());					
		double minCPU = Double.MAX_VALUE;								
						
		// Get the graph and its events per second 
		Partitioning max_partitioning = Partitioning.getPartitioningWithMaxPartition(batch);
		Partition max_partition = max_partitioning.partitions.get(0);
		int vertex_number = max_partition.vertexNumber;
		if (cut_number == vertex_number) {
			System.out.println("Chosen: " + max_partitioning.toString(1));
			return max_partitioning;
		}
				
		/*** Node search ***/
		// Get all not pruned cut sets, construct nearly balanced partitionings, store them in the respective cut sets and store these cut sets in the heap
		ArrayList<CutSet> cutsets = max_partition.getAllCutSets(cut_number);
		int ideal_partition_size = vertex_number / (cut_number+1);
		
		for (CutSet cutset : cutsets) {			
			Partitioning p = max_partition.getNearlyBalancedPartitioning(cutset, ideal_partition_size);
			if (p != null) {
				cutset.partitioning = p;
				heap.add(cutset);			
		}}	
						
		// Find optimal solution
		while (!heap.isEmpty()) {
					
			// Get the next node to process, its costs and children 
			CutSet temp = heap.poll();			
			double temp_cpu = temp.partitioning.getCPUcost(3);
			//System.out.println("Considered: " + temp.toString() + " " + temp.partitioning.toString(3));
						
			// Update the solution and prune the descendants
			if (temp_cpu < minCPU) {
				bestcutset = temp;
				minCPU = temp_cpu;
			}
		}			
		Partitioning result = (bestcutset.cutset.isEmpty()) ? max_partitioning : bestcutset.partitioning; 
		int algorithm = (bestcutset.cutset.isEmpty()) ? 1 : 3;
		System.out.println("Chosen: " + result.toString(algorithm));						
		return result;	
	}
	
	/*** Get CPU cost of this partitioning 
	 * ignoring the CPU cost of graph construction and partitioning 
	 * @param algorithm: 1 for M-CET, 2 for T-CET, 3 for H-CET ***/
	public double getCPUcost (int algorithm) {
		
		/*** M-CET ***/
		if (algorithm == 1) {
			int vertex_number = partitions.get(0).vertexNumber;
			return 2 * Math.pow(3, vertex_number/new Double(3)) * vertex_number;
		} else {
		/*** T-CET ***/
		if (algorithm == 2) {
			int vertex_number = partitions.get(0).vertexNumber;
			int edge_number = partitions.get(0).edgeNumber;
			return edge_number + Math.pow(3, vertex_number/new Double(3));
		} else { 
			/*** H-CET ***/
			double cost_within = 0;		
			int v = 0;
			// CPU cost within partitions
			for (Partition part : partitions) {
				cost_within += part.getCPUcost();
				v += part.vertexNumber;			
			}
			// CPU cost across partitions
			double exp = v/new Double(3);
			double cost_across = 2 * Math.pow(3,exp) * (partitions.size()-1);
			return cost_within + cost_across;
		}}	
	}
	
	/*** Get memory cost of this partitioning 
	 * ignoring the memory cost of graph storage 
	 * @param algorithm: 1 for M-CET, 2 for T-CET, 3 for H-CET ***/
	public double getMEMcost (int algorithm) {
		
		 /*** M-CET ***/
		if (algorithm == 1) {
			int vertex_number = partitions.get(0).vertexNumber;
			return vertex_number;
		} else {
		/*** T-CET ***/
		if (algorithm == 2) { 
			int vertex_number = partitions.get(0).vertexNumber;
			double exp = vertex_number/new Double(3);
			return Math.pow(3, exp) * vertex_number;
		} else { 
			/*** H-CET ***/
			double cost_within = 0;			
			int v = 0;
			// Memory cost within partitions
			for (Partition part : partitions) {
				cost_within += part.getMEMcost();
				v += part.vertexNumber;
			}
			// Memory cost across partitions
			double cost_across = v;					
			return cost_within + cost_across;
		}}		
	}
	
	/*** Get children of this partitioning by splitting a partition in each child ***/
	public ArrayList<Partitioning> getChildrenBySplitting() {
		
		ArrayList<Partitioning> children = new ArrayList<Partitioning>();
		
		for (int i=0; i<partitions.size(); i++) {
			
			// Split ith partition
			Partition partition2split = partitions.get(i);	
			
			//System.out.println("\n2 split: " + partition2split.toString());
			
			ArrayList<CutSet> cutsets = partition2split.getAllCutSets(1);
			ArrayList<Partitioning> split_results = new ArrayList<Partitioning>();
			for (CutSet cutset : cutsets) {
				
				//System.out.println(cut.toString());
				
				Partitioning split_result = partition2split.getPartitioning(cutset);		
				split_results.add(split_result);
				
				//System.out.println("split result: " + split_result.toString(3));
			}
			
			for (Partitioning p : split_results) {
				
				ArrayList<Partition> new_partitions = new ArrayList<Partition>();
			
				// Save all partitions before split partition
				for (int j=0; j<i; j++) {
					Partition old_partition = partitions.get(j);
					new_partitions.add(old_partition);
				}
				// Save split partitions
				new_partitions.addAll(p.partitions);
						
				// Save all partitions after split partition
				for (int j=i+1; j<partitions.size(); j++) {
					Partition old_partition = partitions.get(j);
					new_partitions.add(old_partition);
				}
				// Save new child
				Partitioning child = new Partitioning(new_partitions);
				children.add(child);
			}
		}		
		return children;
	}
	
	/*** Get children of this partitioning by merging a pair of consecutive partitions in each child ***/
	public ArrayList<Partitioning> getChildrenByMerging() {
		
		ArrayList<Partitioning> children = new ArrayList<Partitioning>();
		
		for (int i=0; i+1<partitions.size(); i++) {
			
			ArrayList<Partition> new_partitions = new ArrayList<Partition>();
			
			// Save all partitions before merged partition
			for (int j=0; j<i; j++) {
				Partition old_partition = partitions.get(j);
				new_partitions.add(old_partition);
			}
			// Save merged partition
			Partition first = partitions.get(i);
			Partition second = partitions.get(i+1);
			Partition new_partition = first.merge(second);
			new_partitions.add(new_partition);
			
			// Save all partitions after merged partition
			for (int j=i+2; j<partitions.size(); j++) {
				Partition old_partition = partitions.get(j);
				new_partitions.add(old_partition);
			}
			// Save new child
			Partitioning child = new Partitioning(new_partitions);
			children.add(child);
		}		
		return children;
	}
	
	/*** Get children of this partitioning ***/
	public ArrayList<Partitioning> getChildren (int bin_number, int bin_size) {
		
		ArrayList<Partitioning> children = new ArrayList<Partitioning>();
		int size = this.partitions.size();
		
		if (size < bin_number) {
			
			ArrayList<Partition> previous_partitions = new ArrayList<Partition>();
			
			// Copy all except the last partitions
			for (int i=0; i<size-1; i++) {
				previous_partitions.add(this.partitions.get(i));
			}			
			// Split the last partition in 2: first partition in part of final result, second partition is possibly to split
			Partition partition2split = this.partitions.get(size-1);			
			ArrayList<Partition> split_results = partition2split.split(bin_size);
						
			// Create a new child for each pair of split results
			for (int i=0; i<split_results.size(); i=i+2) {
				ArrayList<Partition> new_partitions = new ArrayList<Partition>();
				new_partitions.addAll(previous_partitions);
				new_partitions.add(split_results.get(i));
				new_partitions.add(split_results.get(i+1));
				Partitioning child = new Partitioning(new_partitions);
				children.add(child);
		}}		
		return children;		
	}
	
	public String toString(int algorithm) {
		String s = "Partition number: " + partitions.size() +
				" CPU: " + getCPUcost(algorithm) + 
				" MEM: " + getMEMcost(algorithm) + "\n";
		for (Partition p : partitions) {
			s += p.toString() + "\n";
		}
		return s;
	}
}