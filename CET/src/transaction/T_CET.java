package transaction;

import iogenerator.OutputFileGenerator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import event.*;
import graph.*;

public class T_CET extends Transaction {
	
	Graph graph;
	
	public T_CET (ArrayList<Event> b, OutputFileGenerator o, CountDownLatch tn, AtomicLong pT, AtomicInteger mMPW) {
		super(b,o,tn,pT,mMPW);			
	}
	
	public void run() {
		
		// Start timer and construct the graph
		long start =  System.currentTimeMillis();
		graph = Graph.constructGraph(batch);
		
		// Estimated CPU and memory costs
		int vertex_number = batch.size();
		int edge_number = graph.edgeNumber;
		double cpu = edge_number + Math.pow(3, vertex_number/new Double(3));
		
		double exp = vertex_number/new Double(3);
		double mem = Math.pow(3, exp) * vertex_number;
		System.out.println("CPU: " + cpu + " MEM: " + mem);		
		
		// Compute results
		computeResults(graph.last_nodes);	
		
		// Stop timer
		long end =  System.currentTimeMillis();
		long processingDuration = end - start;
		processingTime.set(processingTime.get() + processingDuration);
		
		// Output results
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
				EventTrend new_trend = new EventTrend(this_node, this_node, this_node.toString());
				this_node.results.add(new_trend); 			
			}
			
			/*** Recursive case: Copy results from the current node to its previous node and  
			* append this previous node to each copied result ***/
			if (!this_node.isFirst) {			
				
				// System.out.println(this_node.event.id + ": " + this_node.previous.toString());
				
				for (Node next_node : this_node.previous) {
				
					for (EventTrend old_trend : this_node.results) {
						String new_seq = next_node.toString() + ";" + old_trend.sequence;
						EventTrend new_trend = new EventTrend(next_node, old_trend.last_node, new_seq);
						next_node.results.add(new_trend); 
					}														
				
					// Check that following is not in next_level
					if (!next_level_hash.containsKey(next_node.event.id)) {
						next_level_array.add(next_node); 
						next_level_hash.put(next_node.event.id,1);
					}
				}
				// Delete intermediate results
				this_node.results.clear();
			} /*else {
				System.out.println(this_node.toString() + ": " + this_node.resultsToString());
			}*/
		}
				
		// Call this method recursively
		if (!next_level_array.isEmpty()) computeResults(next_level_array);
	}
	
	public void writeOutput2File() {
		
		int memory4results = 0;
				
		if (output.isAvailable()) {
			for (Node first : graph.first_nodes) {
				memory4results += first.printResults(output);
			}
			output.setAvailable();
		}
		// Output of statistics
		int memory = graph.nodes.size() + graph.edgeNumber + memory4results;
		if (maxMemoryPerWindow.get() < memory) maxMemoryPerWindow.getAndAdd(memory);	
	}
}
