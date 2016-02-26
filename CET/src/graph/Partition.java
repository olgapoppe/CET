package graph;

import java.util.ArrayList;
import java.util.Set;

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
	
	public int getEventNumber (String sequence) {
		int number = 1;
		for (int i=0; i<sequence.length(); i++) {
			if (sequence.substring(i,i+1).equals(";")) number++;
		}
		return number;
	}
	
	/*** Returns a minimal partition for events with the same time stamp ***/
	public static Partition getMinPartition (int sec, ArrayList<Event> batch) {
		ArrayList<Node> nodes = new ArrayList<Node>();
		for (Event e : batch) {
			Node n = new Node(e);
			nodes.add(n);
		}
		return new Partition (sec, sec, batch.size(), 0, nodes, nodes);
	}
	
	/*** Get CPU cost of this partition ***/
	public double getCPUcost () {
		return edgeNumber + Math.pow(3, Math.floor(vertexNumber/3));
	}
	
	/*** Get memory cost of this partition ***/
	public double getMEMcost () {
		return vertexNumber * Math.pow(3, Math.floor(vertexNumber/3));
	}
	
	/*** Get actual memory requirement of this partition ***/
	public int getCETlength () {
		int result = 0;
		for (Node last_node : last_nodes) {
			result += getEventNumber(last_node.resultsToString());
		}
		return result;
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
		
		// Connect each vertex in this partition to each vertex in other partition
		for (Node node1 : this.nodes) {
			for (Node node2 : other.nodes) {
				node1.connect(node2);
			}
		}				
		// Create a merged partition
		int start = this.start;
		int end = other.end;
		int vertexes = this.vertexNumber + other.vertexNumber;
		int cut_edges = this.last_nodes.size() * other.first_nodes.size();
		int edges = this.edgeNumber + other.edgeNumber + cut_edges;
		ArrayList<Node> first = this.first_nodes;
		ArrayList<Node> last = other.last_nodes;
		return new Partition(start,end,vertexes,edges,first,last);
	}
	
	public void copyResultsFromLast2First () {
		
		for (Node last_node : last_nodes) {		
			
			System.out.println("LAST: " + last_node.toString() + ": " + last_node.resultsToString());		
			
			Set<Node> first_nodes = last_node.results.keySet();
			for (Node first_node : first_nodes) {
				
				if (!first_node.isLastNode) {
					
					ArrayList<EventTrend> old_trends = new ArrayList<EventTrend>(); 
					if (first_node.results.containsKey(first_node)) old_trends = first_node.results.get(first_node);
					ArrayList<EventTrend> trends2copy = last_node.results.get(first_node);
					ArrayList<EventTrend> all_trends = new ArrayList<EventTrend>();
					all_trends.addAll(old_trends);
					all_trends.addAll(trends2copy);
					first_node.results.put(first_node, all_trends);
				
					System.out.println("FIRST: " + first_node.toString() + ": " + first_node.resultsToString());
		}}}
	}	
	
	public String toString() {
		return start + "-" + end + ": " + vertexNumber + "; " + edgeNumber;
	}
}
