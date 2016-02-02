package transaction;

import iogenerator.OutputFileGenerator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Stack;
import java.util.concurrent.CountDownLatch;
import event.Event;
import graph.Graph;
import graph.Node;

public class Dynamic extends Transaction {
	
	Graph graph;
	
	public Dynamic (ArrayList<Event> b, OutputFileGenerator o, CountDownLatch tn, long start) {
		super(b,o,tn,start);	
		
	}
	
	public void run() {
		
		graph = Graph.constructGraph(batch);		
		for (Node first : graph.first_nodes) {
			Stack<Node> current_sequence = new Stack<Node>();
			computeResults(first,current_sequence);
			writeOutput2File(first);	
		}			
		transaction_number.countDown();
	}
	
	// DFS storing intermediate results
	public void computeResults (Node node, Stack<Node> current_sequence) {       
			
		current_sequence.push(node);
		node.visited = true;		
		System.out.println("pushed " + node.event.id);
	        
		/*** Base case: We hit the end of the graph. Save this node in the result list of this node. ***/
	    if (node.following.isEmpty()) {  
	    	ArrayList<Node> new_result = new ArrayList<Node>();
			new_result.add(node);
	    	node.results.add(new_result);
	    	System.out.println(node.toString() + " is added to " + node.toString());
	    } else {
	    /*** Recursive case: Traverse the following not-visited nodes, copy results from visited nodes and attach this node to them. ***/        	
	       	for(Node following : node.following) {        		
	       		//System.out.println("following of " + node.event.id + " is " + following.event.id);
	       		if (!following.visited) computeResults(following,current_sequence);        		
	       		for (ArrayList<Node> result : following.results) {
	       			ArrayList<Node> new_result = new ArrayList<Node>();
	       			new_result.add(node);
	       			new_result.addAll(result);	       				
	       			node.results.add(new_result);
	       			System.out.println(new_result.toString() + " is added to " + node.toString());
	       		}	       		
	       	}        	
	    }
	    Node top = current_sequence.pop();
	    System.out.println("popped " + top.event.id);
	}
	
	public void writeOutput2File(Node first) {
		int min = Integer.MAX_VALUE;
		int max = Integer.MIN_VALUE;
		try {	
			if (output.isAvailable()) {
				for(ArrayList<Node> sequence : first.results) { 
					//System.out.println(sequence);
					for (Node node : sequence) {
						//System.out.print(event.id + ",");
						if (min > node.event.sec) min = node.event.sec;
						if (max < node.event.sec) max = node.event.sec;
						output.file.append(node.event.print2fileInASeq());
					}
					//System.out.println("\n-----------------------");
					output.file.append("\n");
				}
				output.setAvailable();
			}
		} catch (IOException e) { e.printStackTrace(); }
		if (!first.results.isEmpty()) System.out.println("Number of sequences: " + first.results.size() + " Min: " + min + " Max: " + max);
	}
}
