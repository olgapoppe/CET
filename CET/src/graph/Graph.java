package graph;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;
import event.*;
import transaction.*;

public class Graph {
	
	public ArrayList<Node> nodes;
	public ArrayList<Node> first_nodes;
	public ArrayList<Node> last_nodes;
	
	public Graph () {
		nodes = new ArrayList<Node>();
		first_nodes = new ArrayList<Node>();
		last_nodes = new ArrayList<Node>();
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
 			Graph graph = constructGraph(batch);
 			NonDynamic nd = new NonDynamic(graph);
 			nd.traverse();
 			
		} catch (FileNotFoundException e) {	e.printStackTrace(); } 		
	}
	
	public void connect (Node first, Node second) {
		first.following.add(second);
		second.previous.add(first);
	}
	
	public static Graph constructGraph (ArrayList<Event> events) {		
		
		Graph graph = new Graph();
				
		for (Event event : events) {
			
			//System.out.println(event.toString());
			
			// Create a new node
			Node node = new Node(event);
			
			/*** Case I: Graph is empty. Add this event to the last nodes. ***/
			if (graph.last_nodes.isEmpty()) {
				graph.first_nodes.add(node);
				graph.last_nodes.add(node);
				//System.out.println("Case 1a");
			} else {
				
				ArrayList<Node> new_last_nodes = new ArrayList<Node>();
				ArrayList<Node> old_last_nodes = new ArrayList<Node>();
				for (Node last : graph.last_nodes) {					
			
					/*** Case II: This event is compatible with the last event. Add an edge between last and this. ***/
					if (last.isCompatible(node)) {
						graph.connect(last,node);
						old_last_nodes.add(last);
						new_last_nodes.add(node);
						//System.out.println("Case 2");
						
					} else {
							
						/*** Case III: This event is compatible with a previous event of the last event. Add an edge between previous and this. ***/
						boolean first = true;
						if (last.event.value==event.value) {
							for (Node comp_node : last.previous) {
								if (comp_node.event.sec<event.sec) {
									graph.connect(comp_node,node);
									first = false;
									//System.out.println("Case 3");
								}
							}							 
						}						
						/*** Case I: This event is compatible with no previous event. Add this event to the last nodes. ***/
						if (first) graph.first_nodes.add(node);
						new_last_nodes.add(node);
						//System.out.println("Case 1b");
					}					
				}
				graph.last_nodes.removeAll(old_last_nodes);
				graph.last_nodes.addAll(new_last_nodes);
			}
			// Add the new node to the graph
			graph.nodes.add(node);	
			/*for (Node last2 : graph.last_nodes) { System.out.println("last: " + last2.event.id + ","); }
			for (Node first2 : graph.first_nodes) { System.out.println("first: " + first2.event.id + ","); }*/
		}
		return graph;
	}
}
