package optimizer;

import graph.Partition;

import java.util.ArrayList;

public class Partitioning {
	
	ArrayList<Partition> partitions;
	
	Partitioning (ArrayList<Partition> p) {
		partitions = p;
	}
	
	/**
	 * Get CPU cost of this partitioning
	 */
	public double getCPUcost () {
		double cost4partitions = 0;
		double cost4final_result_construction = 1;
		for (Partition part : partitions) {
			cost4partitions += part.getCPUcost();
			cost4final_result_construction *= Math.pow(3, Math.floor(part.vertexNumber/3));
		}	
		return cost4partitions + cost4final_result_construction;
	}
	
	/**
	 * Get memory cost of this partitioning
	 */
	public double getMEMcost () {
		double cost4partitions = 0;
		for (Partition part : partitions) {
			cost4partitions += part.getCPUcost();			
		}
		return cost4partitions;
	}
	
	/**
	 * Get children of this partitioning by merging a pair of consecutive partitions in each child 
	 */
	public ArrayList<Partitioning> getChildren() {
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
}