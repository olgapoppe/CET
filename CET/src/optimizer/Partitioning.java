package optimizer;

import java.util.ArrayList;
import event.Event;
import graph.Node;
import graph.Partition;

public class Partitioning {
	
	ArrayList<Partition> partitions;
	
	Partitioning (ArrayList<Partition> p) {
		partitions = p;
	}
	
	/*** From the given batch of events construct a partitioning with minimal partitions ***/
	public static Partitioning constructPartitioning (ArrayList<Event> events) {	
		
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
					ArrayList<Node> nodes = new ArrayList<Node>();
					for (Event e : batch) {
						Node n = new Node(e);
						nodes.add(n);
					}
					Partition p = new Partition (curr_sec, curr_sec, batch.size(), 0, nodes, nodes);
					
					System.out.println(p.toString());
					
					parts.add(p);
				}
				// Reset the current second and the batch for the next iteration
				curr_sec = event.sec;
				batch.clear();
				batch.add(event);
			}			
		}
		// Last partition
		if (!batch.isEmpty()) {
			ArrayList<Node> nodes = new ArrayList<Node>();
			for (Event e : batch) {
				Node n = new Node(e);
				nodes.add(n);
			}
			Partition p = new Partition (curr_sec, curr_sec, batch.size(), 0, nodes, nodes);
			
			System.out.println(p.toString());
			
			parts.add(p);
		}
		return new Partitioning(parts);		
	}
	
	/*** Get CPU cost of this partitioning ***/
	public double getCPUcost () {
		double cost4partitions = 0;
		double cost4final_result_construction = 1;
		for (Partition part : partitions) {
			cost4partitions += part.getCPUcost();
			cost4final_result_construction *= Math.pow(3, Math.floor(part.vertexNumber/3));
		}	
		return cost4partitions + cost4final_result_construction;
	}
	
	/*** Get memory cost of this partitioning ***/
	public double getMEMcost () {
		double cost4partitions = 0;
		for (Partition part : partitions) {
			cost4partitions += part.getCPUcost();			
		}
		return cost4partitions;
	}
	
	/*** Get children of this partitioning by merging a pair of consecutive partitions in each child ***/
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