package optimizer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import graph.*;
import event.Window;

public class RandomRoughlyBalancedPartitioning { //extends Partitioner {	
		
	/*public RandomRoughlyBalancedPartitioning (ArrayDeque<Window> w) {
		super(w);
	}
		
	public Partitioning getPartitioning (Partitioning min_partitioning, double memory_limit, int bin_number, int bin_size) {
		
		if (min_partitioning.partitions.size() == 1) {
			return min_partitioning;
		} else {
			
			ArrayList<Partition> partitions = new ArrayList<Partition>();
		
			// For each bin, while it is not full, keep adding new minimal partitions
			Partition current_partition = new Partition(0,0,0,0,new ArrayList<Node>(),new ArrayList<Node>());
			int current_vertex_number = 0;
		
			for (Partition min_partition : min_partitioning.partitions) {
				current_vertex_number += min_partition.vertexNumber;
			
				if (current_vertex_number <= bin_size) {
					
					// Merge with the current partition
					current_partition = current_partition.merge(min_partition);
				} else {
					// Add previous partition to the result
					partitions.add(current_partition);
					
					// Create edges across partitions
					for (Node node1 : current_partition.last_nodes) {
						for (Node node2 : min_partition.first_nodes) {
							node1.connect(node2);			
					}}					
					// Create new partition, add min partition to it
					current_partition = min_partition;
					current_vertex_number = min_partition.vertexNumber;					
			}}	
			// Add last partition and return the result
			partitions.add(current_partition);				
			return new Partitioning(partitions);
		}
	}*/
}
