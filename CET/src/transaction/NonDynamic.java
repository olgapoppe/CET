package transaction;

import iogenerator.OutputFileGenerator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Stack;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import event.Event;
import graph.*;

public class NonDynamic extends Transaction {
	
	Graph graph;
	ArrayList<ArrayList<Node>> results;
	
	public NonDynamic (ArrayList<Event> b, OutputFileGenerator o, CountDownLatch tn, AtomicLong pT) {
		super(b,o,tn,pT);	
		results = new ArrayList<ArrayList<Node>>();
	}
	
	public void run() {
		
		long start =  System.currentTimeMillis();
		graph = Graph.constructGraph(batch);		
		for (Node first : graph.first_nodes) {
			Stack<Node> current_sequence = new Stack<Node>();
			computeResults(first,current_sequence);
		}
		long end =  System.currentTimeMillis();
		long processingDuration = end - start;
		processingTime.set(processingTime.get() + processingDuration);
		
		writeOutput2File();		
		transaction_number.countDown();
	}
	
	// DFS recomputing intermediate results
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
        	//System.out.println("result " + result.toString());
        } else {
        /*** Recursive case: Traverse the following nodes. ***/        	
        	for(Node following : node.following) {        		
        		//System.out.println("following of " + node.event.id + " is " + following.event.id);
        		computeResults(following,current_sequence);        		
        	}        	
        }
        Node top = current_sequence.pop();
        //System.out.println("popped " + top.event.id);
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
