package graph;

import java.util.ArrayList;
import java.util.HashMap;
import optimizer.*;

public class CutSet {
	
	public ArrayList<Integer> cutset;
	public Partitioning partitioning;
	
	public CutSet (ArrayList<Integer> cs) {
		cutset = cs;
	}
	
	public ArrayList<Partition> getNodeNumberPerPartition (int start, int end, HashMap<Integer,ArrayList<Node>> events_per_second) {
		
		ArrayList<Partition> results = new ArrayList<Partition> ();
		int number_of_min_partitions = 0;
		int sec = start;
		int vertex_number = 0;
		int curr_vertex_number = 0;
		int prev_vertex_number = 0;
		int edge_number = 0;
				
		// All partitions except the last
		for (Integer cut : cutset) {
			while (number_of_min_partitions < cut) {
				if (events_per_second.containsKey(sec)) {
					curr_vertex_number = events_per_second.get(sec).size();					
					vertex_number += curr_vertex_number;
					edge_number += prev_vertex_number * curr_vertex_number;
					prev_vertex_number = curr_vertex_number; 
					number_of_min_partitions++;
				}
				sec++;
			} 
			results.add(new Partition(0,0,vertex_number,edge_number,null,null));
			vertex_number = 0;	
			prev_vertex_number = 0;
			edge_number = 0;
		}	
		// Last partition
		while (sec <= end) {
			if (events_per_second.containsKey(sec)) {
				curr_vertex_number = events_per_second.get(sec).size();					
				vertex_number += curr_vertex_number;
				edge_number += prev_vertex_number * curr_vertex_number;
				prev_vertex_number = curr_vertex_number;
			}
			sec++;
		}
		results.add(new Partition(0,0,vertex_number,edge_number,null,null));
		
		return results;
	}
	
	public double getCPUcost (int start, int end, HashMap<Integer,ArrayList<Node>> events_per_second) {
		
		double cost_within = 0;		
		int v = 0;
		// CPU cost within partitions
		ArrayList<Partition> partitions = this.getNodeNumberPerPartition(start, end, events_per_second);
		for (Partition part : partitions) {
					
			cost_within += part.getCPUcost();
			v += part.vertexNumber;			
		}
		// CPU cost across partitions
		double exp = v/new Double(3);
		double cost_across = 2 * Math.pow(3,exp) * (partitions.size()-1);
		return cost_within + cost_across;			
	}
	
	public double getMEMcost (int start, int end, HashMap<Integer,ArrayList<Node>> events_per_second) {
		
		double cost_within = 0;			
		int v = 0;
		// Memory cost within partitions
		ArrayList<Partition> partitions = this.getNodeNumberPerPartition(start, end, events_per_second);
		for (Partition part : partitions) {
			cost_within += part.getMEMcost();
			v += part.vertexNumber;
		}
		// Memory cost across partitions
		double cost_across = v;					
		return cost_within + cost_across;
	}
	
	public boolean isNearlyBalanced (HashMap<Integer,Integer> pruned, HashMap<Integer,ArrayList<Node>> events_per_second) {
		return true;
	}
	
	public boolean isPruned (ArrayList<CutSet> pruned) {
		for (CutSet p : pruned) {
			if (this.cutset.containsAll(p.cutset)) return true;
		}
		return false;
	}	

	public String toString() {
		return cutset.toString();
	}
}
