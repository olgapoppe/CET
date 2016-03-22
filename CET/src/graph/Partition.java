package graph;

import java.util.ArrayDeque;
import java.util.ArrayList;
import event.*;
import optimizer.*;

public class Partition extends Graph {
	
	public String id;
	public int start;
	public int end;
	public int vertexNumber;	
		
	public Partition (int s, int e, int vn, int en, ArrayList<Node> fn, ArrayList<Node> ln) {
		id = s + " " + e;
		start = s;
		end = e;
		
		vertexNumber = vn;
		edgeNumber = en;
		
		first_nodes = fn;
		last_nodes = ln;		
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
	
	public boolean is2write (ArrayDeque<Window> windows, Window window) {
		for (Window w : windows) {
			if (w.contains(this)) {
				return w.equals(window);  
			}
		}
		return false;
	}
	
	public boolean isShared (ArrayDeque<Window> windows) {
		return getSharingWindowNumber(windows)>1;
	}
	 
	/*** Get CPU cost of this partition ***/
	public double getCPUcost (ArrayDeque<Window> windows) {
		double cost = edgeNumber + Math.pow(3, vertexNumber/new Double(3));		
		int windowNumber = getSharingWindowNumber(windows);
		double final_cost = (windowNumber>1) ? cost/windowNumber : cost;
		return final_cost;
	}
	
	/*** Get memory cost of this partition ***/
	public double getMEMcost (ArrayDeque<Window> windows) {
		double cost = vertexNumber * Math.pow(3, Math.floor(vertexNumber/new Double(3))); 		
		int windowNumber = getSharingWindowNumber(windows);
		double final_cost = (windowNumber>1) ? cost/windowNumber : cost;
		return final_cost;
	}
	
	/*** Get actual memory requirement of this partition ***/
	public int getCETlength () {
		int count = 0;
		for (Node first_node : first_nodes) {
			for (EventTrend result : first_node.results) {
				count += result.getEventNumber();
			}
		}
		return count;
	}
	
	/*** Split input partition and return the resulting partitions ***/
	public ArrayList<Partitioning> split () {	
		
		ArrayList<Partitioning> results = new ArrayList<Partitioning>();
		
		// Initial partitions
		Partition first = new Partition(0,0,0,0,new ArrayList<Node>(),new ArrayList<Node>());
		Partition second = this;
		
		// Nodes
		ArrayList<Node> nodes2move = second.first_nodes;
		ArrayList<Node> followingOfNodes2move = new ArrayList<Node>();
		for (Node node2move : nodes2move) {
			for (Node following : node2move.following) {
				if (!followingOfNodes2move.contains(following)) followingOfNodes2move.add(following);			
		}}
		
		// Second
		int secOfNodes2move = nodes2move.get(0).event.sec;
		int secOfFollowingOfNodes2move = (followingOfNodes2move.isEmpty()) ? 0 : followingOfNodes2move.get(0).event.sec;
		
		while (!followingOfNodes2move.isEmpty() && secOfFollowingOfNodes2move <= end) {		
						
			// Vertexes
			int new_first_vn = first.vertexNumber + nodes2move.size();
			int new_second_vn = second.vertexNumber - nodes2move.size();
			
			// Edges
			int oldCutEdges = first.last_nodes.size() * nodes2move.size();
			int newCutEdges = nodes2move.size() * followingOfNodes2move.size();			
			int new_first_en = first.edgeNumber + oldCutEdges;
			int new_second_en = second.edgeNumber - newCutEdges;
			
			// New partitions
			first = new Partition(start,secOfNodes2move,new_first_vn,new_first_en,first_nodes,nodes2move);
			second = new Partition(secOfFollowingOfNodes2move,end,new_second_vn,new_second_en,followingOfNodes2move,last_nodes); 
			
			// New partitioning
			ArrayList<Partition> parts = new ArrayList<Partition>();
			parts.add(first);
			parts.add(second);
			Partitioning result = new Partitioning(parts);
			results.add(result);	
			
			// Reset nodes
			nodes2move = second.first_nodes;
			followingOfNodes2move = new ArrayList<Node>();
			for (Node node2move : nodes2move) {
				for (Node following : node2move.following) {
					if (!followingOfNodes2move.contains(following)) followingOfNodes2move.add(following);			
			}}
			
			// Reset second
			secOfNodes2move = nodes2move.get(0).event.sec;
			secOfFollowingOfNodes2move = (followingOfNodes2move.isEmpty()) ? 0 : followingOfNodes2move.get(0).event.sec;
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
	
	public String toString() {
		return start + "-" + end + ": " + vertexNumber + "; " + edgeNumber;
	}
}
