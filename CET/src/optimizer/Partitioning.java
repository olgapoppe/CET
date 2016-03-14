package optimizer;

import java.util.ArrayDeque;
import java.util.ArrayList;
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
			}			
		}
		// Last partition
		if (!batch.isEmpty()) {
			Partition minimalPartitioning = Partition.getMinPartition(curr_sec, batch);		
			parts.add(minimalPartitioning);
		}
		Partitioning rootPartitioning = new Partitioning(parts);
		System.out.println(rootPartitioning.partitions.size() + " minimal partitions.");
		return rootPartitioning;		
	}
	
	/*** Get CPU cost of this partitioning 
	 * ignoring the CPU cost of graph construction and partitioning ***/
	public double getCPUcost (ArrayDeque<Window> windows) {
		double cost_within = 0;		
		int v = 0;
		// CPU cost within partitions
		for (Partition part : partitions) {
			cost_within += part.getCPUcost(windows);
			v += part.vertexNumber;			
		}	
		// CPU cost across partitions
		int k = partitions.size();
		double cost_across = (k==1) ? 0 : Math.pow(3, Math.floor(v/3)) * (2*k-2);
		return cost_within + cost_across;
	}
	
	/*** Get memory cost of this partitioning 
	 * ignoring the memory cost of graph storage ***/
	public double getMEMcost (ArrayDeque<Window> windows) {
		double cost_within = 0;
		int v = 0;
		// Memory cost within partitions
		for (Partition part : partitions) {
			cost_within += part.getMEMcost(windows);
			v += part.vertexNumber;
		}
		// Memory cost across partitions
		int k = partitions.size();
		double cost_across = (k==1) ? 0 : v;
		return cost_within + cost_across;
	}
	
	/*** Get minimal number of required partitions ***/
	public int getMinNumberOfRequiredPartitions(int vertex_number, int memory_limit) {		
		for (int k=1; k<=vertex_number; k++) {
			double vertex_number_per_partition = Math.floor(new Double(vertex_number)/new Double(k));
			double power = Math.floor(vertex_number_per_partition/new Double(3));
			
			double ideal_memory = k * Math.pow(3, power) * vertex_number_per_partition;
			
			System.out.println("k " + k + 
					" 3^(V_i/3) " + Math.pow(3, power) +
					" V_i " + vertex_number_per_partition +
					" MEM " + ideal_memory);
			// There are fluctruations since k grows faster than the 
			// other values drop down
			
			if (ideal_memory <= memory_limit) return k;
		}		
		return -1;
	}
	
	/*** Get children of this partitioning by splitting a partition in each child ***/
	public ArrayList<Partitioning> getChildrenBySplitting() {
		
		ArrayList<Partitioning> children = new ArrayList<Partitioning>();
		
		for (int i=0; i<partitions.size(); i++) {
			
			// Split ith partition
			Partition partition2split = partitions.get(i);
			ArrayList<Partitioning> split_results = partition2split.split();
			
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
	
	public String toString(ArrayDeque<Window> windows) {
		String s = 
				"Partition number: " + partitions.size() +
				" CPU: " + getCPUcost(windows) + 
				" MEM: " + getMEMcost(windows) + "\n";
		for (Partition p : partitions) {
			s += p.toString() + "\n";
		}
		return s;
	}
}