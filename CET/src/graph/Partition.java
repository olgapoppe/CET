package graph;

import java.util.ArrayList;
import event.Event;

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
		return new Partition (sec, sec, batch.size(), 0, nodes, nodes);
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
	
	/*** Get CPU cost of this partition ***/
	public double getCPUcost () {
		return edgeNumber + Math.pow(3, Math.floor(vertexNumber/3));
	}
	
	/*** Get memory cost of this partition ***/
	public double getMEMcost () {
		return vertexNumber * Math.pow(3, Math.floor(vertexNumber/3));
	}
	
	public String toString() {
		return start + "-" + end + ": " + vertexNumber + "; " + edgeNumber;
	}
}
