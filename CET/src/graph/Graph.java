package graph;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;
import event.*;

public class Graph {
	
	ArrayList<Node> nodes;
	
	Graph () {
		nodes = new ArrayList<Node>();
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
 			// Call the method
 			Graph graph = new Graph();
 			graph.constructGraph(batch);
		} catch (FileNotFoundException e) {	e.printStackTrace(); } 		
	}
	
	public void connect (Node first, Node second) {
		first.following.add(second);
		second.previous.add(first);
	}
	
	public void constructGraph (ArrayList<Event> events) {		
		
		ArrayList<Node> last_nodes = new ArrayList<Node>();
		
		for (Event event : events) {
			
			//System.out.println(event.toString());
			
			// Create a new node
			Node node = new Node(event);
			
			/*** Case I: Graph is empty. Add this event to the last nodes. ***/
			if (last_nodes.isEmpty()) {
				last_nodes.add(node);
				//System.out.println("Case 1a");
			} else {
				
				ArrayList<Node> new_last_nodes = new ArrayList<Node>();
				ArrayList<Node> old_last_nodes = new ArrayList<Node>();
				for (Node last : last_nodes) {					
			
					/*** Case II: This event is compatible with the last event. Add an edge between last and this. ***/
					if (last.isCompatible(node)) {
						connect(last,node);
						old_last_nodes.add(last);
						new_last_nodes.add(node);
						//System.out.println("Case 2");
						
					} else {
							
						/*** Case III: This event is compatible with a previous event of the last event. Add an edge between previous and this. ***/
						if (last.event.value==event.value) {
							for (Node comp_node : last.previous) {
								if (comp_node.event.sec<event.sec) {
									connect(comp_node,node);
									//System.out.println("Case 3");
								}
							}							 
						}						
						/*** Case I: This event is compatible with no previous event. Add this event to the last nodes. ***/
						new_last_nodes.add(node);
						//System.out.println("Case 1b");
					}					
				}
				last_nodes.removeAll(old_last_nodes);
				last_nodes.addAll(new_last_nodes);
			}
			// Add the new node to the graph
			nodes.add(node);	
			/*for (Node last2 : last_nodes) {
				System.out.println(last2.event.id + ",");
			}*/
		}		
	}
}
