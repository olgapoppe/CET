package transaction;

import iogenerator.OutputFileGenerator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
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
				ArrayList<String> new_sequences = new ArrayList<String>();
				new_sequences.add(this_node.toString());
				this_node.results.put(this_node, new_sequences); 
			}
			
			/*** Recursive case: Copy results from the current node to its following node and  
			* append this following node to each copied result ***/
			if (!this_node.isLastNode) {
				for (Node next_node : this_node.following) {
				
					Set<Node> first_nodes = this_node.results.keySet();
					for (Node first_node : first_nodes) {
					
						ArrayList<String> old_sequences = this_node.results.get(first_node);
						ArrayList<String> new_sequences = new ArrayList<String>();
						ArrayList<String> all_sequences = new ArrayList<String>();
					
						for (String seq : old_sequences) {
							String new_seq = seq + ";" + next_node.toString();
							new_sequences.add(new_seq); 
						}
						if (next_node.results.containsKey(first_node)) all_sequences.addAll(next_node.results.get(first_node));
						all_sequences.addAll(new_sequences);
						next_node.results.put(first_node, all_sequences);
					}					
				
					// Check that following is not in next_level
					if (!next_level_hash.containsKey(next_node.event.id)) {
						next_level_array.add(next_node); 
						next_level_hash.put(next_node.event.id,1);
					}
				}
				// Delete intermediate results
				this_node.results.clear();
			}
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
