package transaction;

import iogenerator.OutputFileGenerator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Stack;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import event.Event;
import graph.*;

public class NonDynamic extends Transaction {
	
	Graph graph;
	ArrayList<ArrayList<Node>> results;
	
	public NonDynamic (ArrayList<Event> b, OutputFileGenerator o, CountDownLatch tn, long start) {
		super(b,o,tn,start);	
		results = new ArrayList<ArrayList<Node>>();
	}
	
	public void run() {
		
		graph = Graph.constructGraph(batch);		
		for (Node first : graph.first_nodes) {
			Stack<Node> current_sequence = new Stack<Node>();
			computeResults(first,current_sequence);
		}	
		writeOutput2File();		
		transaction_number.countDown();
	}
	
	// DFS
	public void computeResults (Node node, Stack<Node> current_sequence) {       
		
		current_sequence.push(node);
		//System.out.println("pushed " + node.event.id);
        
		/*** Base case: We hit the end of the graph. Output the current CET. ***/
        if (node.following.isEmpty()) {   
        	ArrayList<Node> result = new ArrayList<Node>();        	
        	Iterator<Node> iter = current_sequence.iterator();
        	while(iter.hasNext()) {
        		Node n = iter.next();
        		result.add(n);
        	}
        	results.add(result);       	       	        	
        } else {
        /*** Recursive case: Update the current CET and traverse the following nodes. ***/        	
        	for(Node following : node.following) {        		
        		//System.out.println("following of " + node.event.id + " is " + following.event.id);
        		computeResults(following,current_sequence);        		
        	}        	
        }
        current_sequence.pop();
        //System.out.println("poped " + top.event.id);
    }
	
	public void writeOutput2File() {
		int min = Integer.MAX_VALUE;
		int max = Integer.MIN_VALUE;
		try {	
			if (output.isAvailable()) {
				for(ArrayList<Node> sequence : results) { // new_results
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
		if (!results.isEmpty()) System.out.println("Number of sequences: " + results.size() + " Min: " + min + " Max: " + max);
	}
}
