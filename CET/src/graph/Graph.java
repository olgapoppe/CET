package graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import event.*;

public class Graph {
	
	public ArrayList<Node> nodes;
	// Events per second
	public HashMap<Integer,ArrayList<Node>> events_per_second;
	public int edgeNumber;
	public ArrayList<Node> first_nodes;
	public ArrayList<Node> last_nodes;  
	public int minPartitionNumber;
		
	public Graph () {
		nodes = new ArrayList<Node>();
		events_per_second = new HashMap<Integer,ArrayList<Node>>();
		edgeNumber = 0;
		first_nodes = new ArrayList<Node>();
		last_nodes = new ArrayList<Node>();
		minPartitionNumber = 0;
	}
	
	public int getNextSec (int sec) {
		int result = Integer.MAX_VALUE;
		Set<Integer> keyset = this.events_per_second.keySet();
		for (int key : keyset) {
			if (key > sec && key < result) 
				result = key;
		}
		//System.out.println("sec: " + sec + " result: " + result);
		return result;
	}
	
	public void connect (Node first, Node second) {
		if (!first.following.contains(second)) {
			first.connect(second);
			edgeNumber++;
		}
	}
	
	public static Graph constructGraph (ArrayList<Event> events) {		
		
		Graph graph = new Graph();
		int curr_sec = -1;
				
		for (Event event : events) {
			
			//System.out.println("--------------" + event.id);
			
			// Update minimal partition number
			if (curr_sec < event.sec) {
				graph.minPartitionNumber++;
				curr_sec = event.sec;
			}
			
			// Create a new node
			Node node = new Node(event);
			
			/*** Case I: This event starts a new sequence. It is a first and a last event. ***/
			if (graph.last_nodes.isEmpty()) {
				graph.first_nodes.add(node);
				node.isFirst = true;
				graph.last_nodes.add(node);				
				//System.out.println(event.id + " starts a new sequence.");
			} else {
				
				ArrayList<Node> new_last_nodes = new ArrayList<Node>();
				ArrayList<Node> old_last_nodes = new ArrayList<Node>();
								
				for (Node last : graph.last_nodes) {		
					
					//System.out.println(" ------------------ \nthis " + event.id + " last " + last.event.id);
			
					/*** Case II: This event is compatible with the last event. Add an edge between last and this. ***/
					if (last.isCompatible(node)) {
						graph.connect(last,node);
						if (!old_last_nodes.contains(last)) old_last_nodes.add(last);
						if (!new_last_nodes.contains(node)) new_last_nodes.add(node);
						//System.out.println(last.event.id + " is connected to " + event.id);
						
					} else {
							
						/*** Case III: This event is compatible with a previous event of the last event. Add an edge between previous and this. ***/
						for (Node comp_node : last.previous) {
							if (comp_node.event.isCompatible(event)) {
								graph.connect(comp_node,node);
								//System.out.println(comp_node.event.id + " is connected to " + event.id);
							} 						
						}
					}
				}
				/*** Case I: This event is compatible with no previous event. Add this event to the last nodes. ***/
				if (node.previous.isEmpty() && !graph.first_nodes.contains(node)) {
					graph.first_nodes.add(node);							
					node.isFirst = true;
					//System.out.println(event.id + " starts a new sequence.");
				}
				if (!new_last_nodes.contains(node)) {
					new_last_nodes.add(node);							
				}					
								
				graph.last_nodes.removeAll(old_last_nodes);
				graph.last_nodes.addAll(new_last_nodes);
			}
			// Add the new node to the graph
			graph.nodes.add(node);
			
			int sec = node.event.sec;
			ArrayList<Node> ns = (graph.events_per_second.containsKey(sec)) ? graph.events_per_second.get(sec) : new ArrayList<Node>();
			ns.add(node);
			graph.events_per_second.put(sec,ns);			
		}
		//for (Node node : graph.nodes) { System.out.println(node.toString()); }	
		//System.out.println(graph.printEventNumberPerSecond());
		return graph;
	}	
	
	public String printEventNumberPerSecond() {
		String result = "";
		Set<Integer> keyset = this.events_per_second.keySet();
		for (int key : keyset) {
			result += key + " : " + this.events_per_second.get(key).size() + ", ";
		}
		return result;
	}
}
