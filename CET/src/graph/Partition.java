package graph;

import java.util.ArrayList;

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
	
	/**
	 * Merge two input partitions and return the resulting partition
	 */
	public Partition merge (Partition other) {
		int start = this.start;
		int end = other.end;
		int vertexes = this.vertexNumber + other.vertexNumber;
		int cut_edges = 0;
		for (Node node : this.last_nodes) {
			cut_edges += node.following.size();
		}		
		int edges = this.edgeNumber + other.edgeNumber + cut_edges;
		ArrayList<Node> first = this.first_nodes;
		ArrayList<Node> last = other.last_nodes;
		return new Partition(start,end,vertexes,edges,first,last);
	}
	
	/**
	 * Get CPU cost of this partition
	 */
	public double getCPUcost () {
		return edgeNumber + Math.pow(3, Math.floor(vertexNumber/3));
	}
	
	/**
	 * Get memory cost of this partition
	 */
	public double getMEMcost () {
		return vertexNumber * Math.pow(3, Math.floor(vertexNumber/3));
	}
}