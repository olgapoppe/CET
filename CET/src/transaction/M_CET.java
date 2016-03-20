package transaction;

import iogenerator.OutputFileGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Stack;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import event.Event;
import graph.*;

public class M_CET extends Transaction {
	
	Graph graph;
	// A result is a string of comma separated event ids
	ArrayList<String> results;
	
	public M_CET (ArrayList<Event> b, OutputFileGenerator o, CountDownLatch tn, AtomicLong pT, AtomicInteger mMPW) {
		super(b,o,tn,pT,mMPW);	
		results = new ArrayList<String>();
	}
	
	public void run() {
		
		long start =  System.currentTimeMillis();
		graph = Graph.constructGraph(batch);	
		
		/*double cpu = 2 * Math.pow(3, Math.floor(batch.size()/3)) * batch.size();
		double mem = batch.size() + graph.edgeNumber;
		System.out.println("CPU: " + cpu + " MEM: " + mem);*/
		
		int maxSeqLength = 0;
		for (Node first : graph.first_nodes) {
			Stack<Node> current_sequence = new Stack<Node>();
			maxSeqLength = computeResults(first,current_sequence,maxSeqLength);
		}
		long end =  System.currentTimeMillis();
		long processingDuration = end - start;
		processingTime.set(processingTime.get() + processingDuration);
		
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
        		computeResults(following,current_sequence,maxSeqLength);        		
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
		if (maxMemoryPerWindow.get() < memory) maxMemoryPerWindow.getAndAdd(memory);	
	}
}
