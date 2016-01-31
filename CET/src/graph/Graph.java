package graph;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import event.*;

public class Graph {
	
	public ArrayList<Node> nodes;
	public ArrayList<Node> first_nodes;
	public HashMap<Integer,ArrayList<Node>> last_nodes; // Maps value to the last events in sequences with this value 
	
	public Graph () {
		nodes = new ArrayList<Node>();
		first_nodes = new ArrayList<Node>();
		last_nodes = new HashMap<Integer,ArrayList<Node>>();
	}
	
	public void connect (Node first, Node second) {
		if (!first.following.contains(second)) {
			first.following.add(second);
			second.previous.add(first);
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
				//System.out.println("Case 1a");
			} else {
				
				ArrayList<Node> new_last_nodes = new ArrayList<Node>();
				ArrayList<Node> old_last_nodes = new ArrayList<Node>();
				ArrayList<Node> lnodes = graph.last_nodes.get(event.value);
				
				for (Node last : lnodes) {					
			
					/*** Case II: This event is compatible with the last event. Add an edge between last and this. ***/
					if (last.isCompatible(node)) {
						graph.connect(last,node);
						old_last_nodes.add(last);
						new_last_nodes.add(node);
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
						new_last_nodes.add(node);
						//System.out.println("Case 1b");
					}			
				}
				lnodes.removeAll(old_last_nodes);
				lnodes.addAll(new_last_nodes);
				graph.last_nodes.put(event.value, lnodes);
			}
			// Add the new node to the graph
			graph.nodes.add(node);	
		}
		for (Node node : graph.nodes) { System.out.println(node.toString()); }		
		return graph;
	}
	
	public static void main (String args[]) {	
		try {
			// Input
			String inputfile = "src\\iofiles\\stream1.txt";
			Scanner scanner = new Scanner(new File(inputfile));		
			String line = scanner.nextLine();
			ArrayList<Event> batch = new ArrayList<Event>();
			Event event = Event.parse(line); 			
			while (event != null) { 				
				batch.add(event);
				if (scanner.hasNextLine()) {		 				
					line = scanner.nextLine();   
					event = Event.parse(line);		 				
				} else {
					event = null;		 				
				}
			}
			scanner.close(); 	
			// Call the methods
			constructGraph(batch);
			//NonDynamic nd = new NonDynamic(graph);
			//nd.traverse();			
		} catch (FileNotFoundException e) {	e.printStackTrace(); } 		
	}
}
