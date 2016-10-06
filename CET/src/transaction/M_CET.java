package transaction;

import iogenerator.OutputFileGenerator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Stack;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import event.*;
import graph.*;

public class M_CET extends Transaction {
	
	Graph graph;
	// A result is a string of comma separated event ids
	ArrayList<String> results;
	
	public M_CET (Window w, OutputFileGenerator o, CountDownLatch tn, AtomicLong time, AtomicInteger mem) {
		super(w,o,tn,time,mem);	
		results = new ArrayList<String>();
	}
	
	public void run() {
		
		// Start timer and construct the graph
		long start =  System.currentTimeMillis();
		graph = Graph.constructGraph(window.events);	
		
		// Estimated CPU and memory costs
		int vertex_number = window.events.size();
		double estimated_cpu = 2 * Math.pow(3, vertex_number/new Double(3)) * vertex_number;	
		
		int estimated_mem = vertex_number;
		System.out.println("CPU: " + estimated_cpu + " MEM: " + estimated_mem);
		
		// Compute results
		int maxSeqLength = 0;
		for (Node first : graph.first_nodes) {
			Stack<Node> current_sequence = new Stack<Node>();
			maxSeqLength = computeResults(first,current_sequence,maxSeqLength);
		}
		
		// Stop timer
		long end =  System.currentTimeMillis();
		long duration = end - start;
		total_cpu.set(total_cpu.get() + duration);
		
		// Output results
		writeOutput2File(maxSeqLength);		
		transaction_number.countDown();
	}
	
	// DFS recomputing intermediate results
	public int computeResults (Node node, Stack<Node> current_sequence, int maxSeqLength) {       
		
		current_sequence.push(node);
		//System.out.println("pushed " + node.event.id);
        
		/*** Base case: We hit the end of the graph. Output the current CET. ***/
        if (node.following.isEmpty()) {   
        	String result = "";        	
        	Iterator<Node> iter = current_sequence.iterator();
        	while(iter.hasNext()) {
        		Node n = iter.next();
        		result += n.toString() + ";";
        	}
        	int eventNumber = getEventNumber(result);
			if (maxSeqLength < eventNumber) maxSeqLength = eventNumber;	
        	//results.add(result);  
        	
			// System.out.println("result " + result);
			
        } else {
        /*** Recursive case: Traverse the following nodes. ***/        	
        	for(Node following : node.following) {        		
        		//System.out.println("following of " + node.event.id + " is " + following.event.id);
        		maxSeqLength = computeResults(following,current_sequence,maxSeqLength);        		
        	}        	
        }
        Node top = current_sequence.pop();
        //System.out.println("popped " + top.event.id);
        
        return maxSeqLength;
    }
	
	public void writeOutput2File(int maxSeqLength) {
		
		if (output.isAvailable()) {			
			for(String sequence : results) {							 				
				try { output.file.append(sequence + "\n"); } catch (IOException e) { e.printStackTrace(); }
				int eventNumber = getEventNumber(sequence);
				if (maxSeqLength < eventNumber) maxSeqLength = eventNumber;			
			}
			output.setAvailable();
		}		
		// Output of statistics
		int memory = graph.nodes.size() + graph.edgeNumber + maxSeqLength;
		total_mem.set(total_mem.get() + memory);
		//if (total_mem.get() < memory) total_mem.getAndAdd(memory);	
	}
}
