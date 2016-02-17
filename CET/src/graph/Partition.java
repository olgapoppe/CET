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
	public Partition merge (Partition p1, Partition p2) {
		int start = p1.start;
		int end = p2.end;
		int vertexes = p1.vertexNumber + p2.vertexNumber;
		int cut_edges = 0;
		for (Node node : p1.last_nodes) {
			cut_edges += node.following.size();
		}		
		int edges = p1.edgeNumber + p2.edgeNumber + cut_edges;
		ArrayList<Node> first = p1.first_nodes;
		ArrayList<Node> last = p2.last_nodes;
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
