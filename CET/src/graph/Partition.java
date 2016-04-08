package graph;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;

import optimizer.Partitioning;
import event.*;

public class Partition extends Graph {
	
	public String id;
	public int start;
	public int end;
	public int vertexNumber;	
	public ArrayList<EventTrend> results; 
		
	public Partition (int s, int e, int vn, int en, ArrayList<Node> fn, ArrayList<Node> ln) {
		id = s + " " + e;
		start = s;
		end = e;
		
		vertexNumber = vn;
		edgeNumber = en;
		
		first_nodes = fn;
		last_nodes = ln;		
		
		results = new ArrayList<EventTrend>();
	}
	
	public boolean equals (Object o) {
		Partition other = (Partition) o;
		return this.id.equals(other.id);
	}
	
	/*** Returns a minimal partition for events with the same time stamp ***/
	public static Partition getMinPartition (int sec, ArrayList<Event> batch) {
		ArrayList<Node> nodes = new ArrayList<Node>();
		for (Event e : batch) {
			Node n = new Node(e);
			nodes.add(n);
		} 
		Partition result = new Partition (sec, sec, batch.size(), 0, nodes, nodes);
		result.nodes = nodes;
		return result;
	}
	
	public int getSharingWindowNumber (ArrayDeque<Window> windows) {
		int count = 0;
		for (Window window : windows) {
			if (window.contains(this)) count++;  
		}
		return count;
	}
	
	public boolean isShared (ArrayDeque<Window> windows) {
		return getSharingWindowNumber(windows)>1;
	}
	 
	/*** Get CPU cost of this partition ***/
	public double getCPUcost () {
		double exp = vertexNumber/new Double(3);
		double cost = edgeNumber + Math.pow(3,exp);		
		/*int windowNumber = getSharingWindowNumber(windows);
		double final_cost = (windowNumber>1) ? cost/windowNumber : cost;*/
		return cost;
	}
	
	/*** Get memory cost of this partition ***/
	public double getMEMcost () {
		double exp = vertexNumber/new Double(3);
		double cost = vertexNumber * Math.pow(3, exp); 		
		/*int windowNumber = getSharingWindowNumber(windows);
		double final_cost = (windowNumber>1) ? cost/windowNumber : cost;*/
		return cost;
	}
	
	/*** Get actual memory requirement of this partition ***/
	public int getCETlength () {
		int count = 0;
		for (Node first_node : first_nodes) {
			for (EventTrend result : first_node.results) {
				count += result.getEventNumber();
		}}
		return count;
	}
	
	/*** Split input partition and return the resulting partitions ***/
	public ArrayList<Partition> split (int bin_size) {
		
		ArrayList<Partition> results = new ArrayList<Partition>();
		
		// Find the time points where to cut
		ArrayList<Node> previous_nodes = new ArrayList<Node>();
		ArrayList<Node> current_nodes = this.first_nodes;
		int count = current_nodes.size();
		int prev_sec = 0;
		int new_sec = 0;
		int vertex_number = 0;
		int edge_number = 0;
		int cut_edges = 0;
				
		while (count<bin_size) {
			
			ArrayList<Node> new_current_nodes = new ArrayList<Node>();
			for (Node current_node : current_nodes) {
				for (Node following : current_node.following) {
					if (!new_current_nodes.contains(following)) new_current_nodes.add(following);
			}}
			count += new_current_nodes.size();
			prev_sec = current_nodes.get(0).event.sec;
			new_sec = new_current_nodes.get(0).event.sec;
			vertex_number += current_nodes.size();
			edge_number += previous_nodes.size() * current_nodes.size();
			cut_edges = current_nodes.size() * new_current_nodes.size();
			
			previous_nodes = current_nodes;
			current_nodes = new_current_nodes;
		}
		// Cut the graph at these time points
		// 1st pair of partitions is created 
		Partition first = new Partition(this.start, prev_sec, vertex_number, edge_number, this.first_nodes, previous_nodes);
		Partition second = new Partition(new_sec, this.end, this.vertexNumber-vertex_number, this.edgeNumber-edge_number-cut_edges, current_nodes, this.last_nodes);
		results.add(first);
		results.add(second);
			
		// 2nd pair of partitions is created
		if (vertex_number<bin_size) {
			ArrayList<Node> new_current_nodes = new ArrayList<Node>();
			for (Node current_node : current_nodes) {
				for (Node following : current_node.following) {
					if (!new_current_nodes.contains(following)) new_current_nodes.add(following);				
			}}
			prev_sec = current_nodes.get(0).event.sec;
			new_sec = new_current_nodes.get(0).event.sec;
			vertex_number += current_nodes.size();
			edge_number += previous_nodes.size() * current_nodes.size();
			cut_edges = current_nodes.size() * new_current_nodes.size();			
			
			Partition third = new Partition(this.start, prev_sec, vertex_number, edge_number, this.first_nodes, current_nodes);
			Partition forth = new Partition(new_sec, this.end, this.vertexNumber-vertex_number, this.edgeNumber-edge_number-cut_edges, new_current_nodes, this.last_nodes);
			results.add(third);
			results.add(forth);
		}		
		return results;
	}
	
	/*** Merge two input partitions and return the resulting partition ***/
	public Partition merge (Partition other) {		
		
		// Connect a last vertex in this partition to a first vertex in other partition
		for (Node node1 : this.last_nodes) {
			for (Node node2 : other.first_nodes) {
				node1.connect(node2);			
		}}				
		// Create a merged partition
		int start = this.start;
		int end = other.end;
		int vertexes = this.vertexNumber + other.vertexNumber;
		int cut_edges = this.last_nodes.size() * other.first_nodes.size();
		int edges = this.edgeNumber + other.edgeNumber + cut_edges;
		ArrayList<Node> first = (!this.first_nodes.isEmpty()) ? this.first_nodes : other.first_nodes;
		ArrayList<Node> last = other.last_nodes;
		Partition result = new Partition(start,end,vertexes,edges,first,last);
		
		// Merge the nodes of both partitions
		ArrayList<Node> merged_nodes = new ArrayList<Node>();
		merged_nodes.addAll(this.nodes);
		merged_nodes.addAll(other.nodes);
		result.nodes = merged_nodes;
		
		// Return the resulting partition
		return result; 
	}
	
	/*** Get all combinations of numbers from 1 to max of length n  ***/
	public ArrayList<CutSet> getAllCutSets (int n) {	
		
		// Result accumulator
		ArrayList<CutSet> results = new ArrayList<CutSet>();
			
		// Fill input array with numbers
		int max = this.minPartitionNumber-1;
		//System.out.println("Max: " + max);
		
		if (max>0) {
		
			int arr[] = new int[max];		
			for (int i=1; i<=max; i++) {
				arr[i-1] = i;
			}	
				
			// A temporary array to store all combination one by one
			int data[] = new int[n];		

			// Get all combinations using temporary array 'data[]'
			results = getAllCutSetsAux(arr, data, 0, arr.length-1, 0, n, results, new ArrayList<CutSet>());
		}
		return results;
	}
	
	/*** Get all combinations of numbers from 1 to max of length n  ***/
	public ArrayList<CutSet> getAllNotPrunedCutSets (int n, ArrayList<CutSet> pruned) {	
		
		// Result accumulator
		ArrayList<CutSet> results = new ArrayList<CutSet>();
			
		// Fill input array with numbers
		int max = this.minPartitionNumber-1;
		//System.out.println("Max: " + max);
		
		if (max>0) {
		
			int arr[] = new int[max];		
			for (int i=1; i<=max; i++) {
				arr[i-1] = i;
			}	
				
			// A temporary array to store all combination one by one
			int data[] = new int[n];		

			// Get all combinations using temporary array 'data[]'
			results = getAllCutSetsAux(arr, data, 0, arr.length-1, 0, n, results, pruned);
		}
		return results;
	}
	
	public ArrayList<CutSet> getAllCutSetsAux (int arr[], int data[], int start, int end, int index, int r, 
			ArrayList<CutSet> results, ArrayList<CutSet> pruned) {
		
		// Current combination is done, save it in results
		if (index == r) {
			CutSet result = new CutSet(new ArrayList<Integer> ());
			for (int j=0; j<r; j++) {
				//System.out.print(data[j]+" ");
				result.cutset.add(data[j]);
			}
			//System.out.println("");
			
			if (!result.cutset.contains(0) && !result.isPruned(pruned)) results.add(result);						
			return results;
		}

		// replace index with all possible elements. The condition
		// "end-i+1 >= r-index" makes sure that including one element
		// at index will make a combination with remaining elements
		// at remaining positions
		for (int i=start; i<=end && end-i+1 >= r-index; i++) {
			data[index] = arr[i];
			results = getAllCutSetsAux(arr, data, i+1, end, index+1, r, results, pruned);
		}
		return results;
	}
	
	public Partitioning getPartitioning (CutSet cutset) {
		
		ArrayList<Partition> parts = new ArrayList<Partition>();
		
		// Set local variables
		int start = this.start;
		int vertex_number = 0;
		int prev_node_number = 0;
		int edge_number = 0;
		ArrayList<Node> first_nodes = this.first_nodes;
		ArrayList<Node> last_nodes = this.last_nodes;
		int minPartitionNumber = 0;
				
		int index = 0;
		int cut = cutset.cutset.get(index);
		int cut_count = 1;		
			
		// Add seconds to current partition until the next cut
		for (int sec=start; sec<=this.end; sec=this.getNextSec(sec)) {
			
			if (events_per_second.containsKey(sec)) {
				
				ArrayList<Node> nodes = events_per_second.get(sec);
						
				if (cut_count == cut) {
				
					vertex_number += nodes.size();
					edge_number += prev_node_number * nodes.size();
					minPartitionNumber++;
					Partition p = new Partition(start,sec,vertex_number,edge_number,first_nodes,nodes);
					p.events_per_second = this.events_per_second;
					p.minPartitionNumber = minPartitionNumber;
					parts.add(p);
					//System.out.println(p.toString());
				
					int next_sec = this.getNextSec(sec);
					start = next_sec;
					vertex_number = 0;
					edge_number = 0;
					prev_node_number = 0;
					if (next_sec<=this.end) first_nodes = events_per_second.get(next_sec);
					if (index+1<=cutset.cutset.size()-1) cut = cutset.cutset.get(++index);
					minPartitionNumber = 0;
				} else {
					vertex_number += nodes.size();
					edge_number += prev_node_number * nodes.size();
					prev_node_number = nodes.size();
					minPartitionNumber++;
				}
				cut_count++;
			}
		}	
		// Add last partition
		Partition p = new Partition(start,this.end,vertex_number,edge_number,first_nodes,last_nodes);
		p.events_per_second = this.events_per_second;
		p.minPartitionNumber = minPartitionNumber;
		parts.add(p);
		//System.out.println(p.toString());
		
		Partitioning partitioning = new Partitioning(parts);
		return partitioning;
	}
	
	public Partitioning getNearlyBalancedPartitioning (CutSet cutset, int ideal_partition_size) {
		
		ArrayList<Partition> parts = new ArrayList<Partition>();
		
		// Set local variables
		int start = this.start;
		int vertex_number = 0;
		int prev_node_number = 0;
		int edge_number = 0;
		ArrayList<Node> first_nodes = this.first_nodes;
		ArrayList<Node> last_nodes = this.last_nodes;
		int minPartitionNumber = 0;
				
		int index = 0;
		int cut = cutset.cutset.get(index);
		int cut_count = 1;
		boolean isNearlyBalanced = true;
		boolean isFirstPartition = true;
			
		// Add seconds to current partition until the next cut
		for (int sec=start; sec<=this.end; sec=this.getNextSec(sec)) {
			
			if (events_per_second.containsKey(sec)) {
				
				ArrayList<Node> nodes = events_per_second.get(sec);
										
				if (cut_count == cut) {
				
					vertex_number += nodes.size();
					edge_number += prev_node_number * nodes.size();
					minPartitionNumber++;
					
					// Check that new partition is nearly balanced
					isNearlyBalanced = (isFirstPartition) ? 
						(vertex_number - nodes.size() <= ideal_partition_size) : 
						(vertex_number - first_nodes.size() <= ideal_partition_size || vertex_number - nodes.size() <= ideal_partition_size);
					if (!isNearlyBalanced) return null;
					
					// Add new partition to the results
					Partition p = new Partition(start,sec,vertex_number,edge_number,first_nodes,nodes);
					p.events_per_second = this.events_per_second;
					p.minPartitionNumber = minPartitionNumber;
					parts.add(p);
					//System.out.println(p.toString());
				
					int next_sec = this.getNextSec(sec);
					start = next_sec;
					vertex_number = 0;
					edge_number = 0;
					prev_node_number = 0;					
					if (next_sec<=this.end) {
						first_nodes = events_per_second.get(next_sec);
						isFirstPartition = false;
					}
					if (index+1<=cutset.cutset.size()-1) cut = cutset.cutset.get(++index);
					minPartitionNumber = 0;					
				} else {
					vertex_number += nodes.size();
					edge_number += prev_node_number * nodes.size();
					prev_node_number = nodes.size();
					minPartitionNumber++;					
				}
				cut_count++;				
			}
		}	
		// Check that last partition is nearly balanced
		isNearlyBalanced = vertex_number - first_nodes.size() <= ideal_partition_size;
		if (!isNearlyBalanced) return null;
		
		// Add last partition
		Partition p = new Partition(start,this.end,vertex_number,edge_number,first_nodes,last_nodes);
		p.events_per_second = this.events_per_second;
		p.minPartitionNumber = minPartitionNumber;
		parts.add(p);
		//System.out.println(p.toString());
		
		Partitioning partitioning = new Partitioning(parts);
		return partitioning;
	}
	
	public String toString() {
		return start + "-" + end + ": " + vertexNumber + "; " + edgeNumber;
	}
}
