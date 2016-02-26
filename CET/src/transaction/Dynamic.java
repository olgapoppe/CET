package transaction;

import iogenerator.OutputFileGenerator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import event.*;
import graph.*;

public class Dynamic extends Transaction {
	
	Graph graph;
	
	public Dynamic (ArrayList<Event> b, OutputFileGenerator o, CountDownLatch tn, AtomicLong pT, AtomicInteger mMPW) {
		super(b,o,tn,pT,mMPW);			
	}
	
	public void run() {
		
		long start =  System.currentTimeMillis();
		graph = Graph.constructGraph(batch);
		computeResults(graph.first_nodes);		
		long end =  System.currentTimeMillis();
		long processingDuration = end - start;
		processingTime.set(processingTime.get() + processingDuration);
		
		writeOutput2File();
		transaction_number.countDown();
	}
	
	// BFS storing intermediate results in all nodes at the current level
	public static void computeResults (ArrayList<Node> current_level) { 
		
		// Array for recursive call of this method
		ArrayList<Node> next_level_array = new ArrayList<Node>();
		
		// Hash for quick lookup of saved nodes
		HashMap<Integer,Integer> next_level_hash = new HashMap<Integer,Integer>();
		
		for (Node this_node : current_level) {
			
			/*** Base case: Create the results for the first nodes ***/
			if (this_node.results.isEmpty()) {
				EventTrend et = new EventTrend(this_node, this_node.toString());
				this_node.results.add(et); 
			}
			
			/*** Recursive case: Copy results from the current node to its following node and  
			* append this following node to each copied result ***/
			for (Node next_node : this_node.following) {
				for (EventTrend et : this_node.results) {	
					String new_seq = et.sequence + ";" + next_node.toString();
					EventTrend new_et = new EventTrend(et.first_node, new_seq);
					next_node.results.add(new_et); 
				}	
				// Check that following is not in next_level
				if (!next_level_hash.containsKey(next_node.event.id)) {
					next_level_array.add(next_node); 
					next_level_hash.put(next_node.event.id,1);
				}
			}
			// Delete intermediate results
			if (!this_node.isLastNode) this_node.results.clear();
		}		
		// Call this method recursively
		if (!next_level_array.isEmpty()) computeResults(next_level_array);
	}
	
	public void writeOutput2File() {
		
		int memory4results = 0;
				
		if (output.isAvailable()) {
			for (Node last : graph.last_nodes) {
				memory4results += last.printResults(output);
			}
			output.setAvailable();
		}
		// Output of statistics
		int memory = graph.nodes.size() + graph.edgeNumber + memory4results;
		if (maxMemoryPerWindow.get() < memory) maxMemoryPerWindow.getAndAdd(memory);	
	}
}
