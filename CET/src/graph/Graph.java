package graph;

import java.util.ArrayList;
import java.util.HashMap;
import event.*;

public class Graph {
	
	public ArrayList<Node> nodes;
	public int edgeNumber;
	public ArrayList<Node> first_nodes;
	// Maps value to the last events in sequences with this value
	public HashMap<Integer,ArrayList<Node>> last_nodes;  
	
	public Graph () {
		nodes = new ArrayList<Node>();
		edgeNumber = 0;
		first_nodes = new ArrayList<Node>();
		last_nodes = new HashMap<Integer,ArrayList<Node>>();
	}
	
	public void connect (Node first, Node second) {
		if (!first.following.contains(second)) {
			first.following.add(second);
			second.previous.add(first);
			first.isLastNode = false;
			second.isLastNode = true;
			edgeNumber++;
		}
	}
	
	public static Graph constructGraph (ArrayList<Event> events) {		
		
		Graph graph = new Graph();
				
		for (Event event : events) {
			
			//System.out.println("--------------" + event.id);
			
			// Create a new node
			Node node = new Node(event);
			
			/*** Case I: This event starts a new sequence. It is a first and a last event. ***/
			if (!graph.last_nodes.containsKey(event.value)) {
				graph.first_nodes.add(node);
				ArrayList<Node> nodes = new ArrayList<Node>();
				nodes.add(node);
				graph.last_nodes.put(event.value,nodes);
				node.isLastNode = true;
				//System.out.println(event.id + " starts a new sequence.");
			} else {
				
				ArrayList<Node> new_last_nodes = new ArrayList<Node>();
				ArrayList<Node> old_last_nodes = new ArrayList<Node>();
				ArrayList<Node> lnodes = graph.last_nodes.get(event.value);
				
				for (Node last : lnodes) {					
			
					/*** Case II: This event is compatible with the last event. Add an edge between last and this. ***/
					if (last.isCompatible(node)) {
						graph.connect(last,node);
						if (!old_last_nodes.contains(last)) old_last_nodes.add(last);
						if (!new_last_nodes.contains(node)) new_last_nodes.add(node);
						//System.out.println(last.event.id + " is connected to " + event.id);
						
					} else {
							
						/*** Case III: This event is compatible with a previous event of the last event. Add an edge between previous and this. ***/
						boolean first = true;
						for (Node comp_node : last.previous) {
							if (comp_node.event.sec<event.sec) {
								graph.connect(comp_node,node);
								first = false;
								//System.out.println(comp_node.event.id + " is connected to " + event.id);
							}
						}							 
						/*** Case I: This event is compatible with no previous event. Add this event to the last nodes. ***/
						if (first) graph.first_nodes.add(node);
						if (!new_last_nodes.contains(node)) {
							new_last_nodes.add(node);
							node.isLastNode = true;
						}
						//System.out.println(event.id + " starts a new sequence.");
					}			
				}
				lnodes.removeAll(old_last_nodes);
				lnodes.addAll(new_last_nodes);
				graph.last_nodes.put(event.value, lnodes);
			}
			// Add the new node to the graph
			graph.nodes.add(node);	
		}
		//for (Node node : graph.nodes) { System.out.println(node.toString()); }		
		return graph;
	}	
}
