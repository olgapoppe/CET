package optimizer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import graph.*;
import event.Window;

public class BalancedPartitions extends Partitioner {	
		
	public BalancedPartitions (ArrayDeque<Window> w) {
		super(w);
	}
		
	public Partitioning getPartitioning (Partitioning min_partitions, int bin_size) {
		
		ArrayList<Partition> partitions = new ArrayList<Partition>();
		
		// for each bin, while it is not full, keep adding new minimal partitions
		int current_vertex_number = 0;
		for (Partition min_partition : min_partitions.partitions) {
			current_vertex_number += min_partition.vertexNumber;
			if (current_vertex_number < bin_size) {
				// merge with the current partition
			} else {
				// add previous partition to the result
				// create new partition, add min partition to it
				current_vertex_number = min_partition.vertexNumber;
			}
		}	
		
		// return the partitioning consisting of bins		
		return new Partitioning(partitions);
	}
}
