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
	
	public T_CET (Window w, OutputFileGenerator o, CountDownLatch tn, AtomicLong time, AtomicInteger mem) {
		super(w,o,tn,time,mem);			
	}
	
	public void run() {
		
		// Start timer and construct the graph
		long start =  System.currentTimeMillis();
		graph = Graph.constructGraph(window.events);
		
		/*// Estimated CPU and memory costs
		int vertex_number = batch.size();
		int edge_number = graph.edgeNumber;
		//System.out.println("Edge number: " + edge_number);
		double estimated_cpu = edge_number + Math.pow(3, vertex_number/new Double(3));
		
		double exp = vertex_number/new Double(3);
		double estimated_mem = Math.pow(3, exp) * vertex_number;
		System.out.println("CPU: " + estimated_cpu + " MEM: " + estimated_mem);*/		
		
		// Compute results
		computeResults(graph.last_nodes,false,new ArrayList<EventTrend>());	
		
		// Stop timer
		long end =  System.currentTimeMillis();
		long duration = end - start;
		total_cpu.set(total_cpu.get() + duration);
		
		// Output results
		writeOutput2File();
		transaction_number.countDown();
	}
	
	// BFS storing intermediate results in all nodes at the current level
	public static ArrayList<EventTrend> computeResults (ArrayList<Node> current_level, boolean writes, ArrayList<EventTrend> partitionResults) { 
		
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
			} else {
				// Add all results from a first node to the results of this partition
				if (writes) {
					partitionResults.addAll(this_node.results);
					//System.out.println(this_node.toString() + ": " + this_node.resultsToString());
				}
			}
		}
				
		// Call this method recursively
		if (!next_level_array.isEmpty()) computeResults(next_level_array, writes, partitionResults);
		
		return partitionResults;
	}
	
	public void writeOutput2File() {
		
		int memory4results = 0;
		int count = 0;
		int max = 0;
				
		if (output.isAvailable()) {
			for (Node first : graph.first_nodes) {
				memory4results += first.printResults(output);
				count += first.results.size();
				int length = first.getMaxLength();
				if (max<length) max = length;			
			}
			output.setAvailable();
		}
		// Output of statistics
		int memory = graph.nodes.size() + graph.edgeNumber + memory4results;
		total_mem.set(total_mem.get() + memory);
		//if (total_mem.get() < memory) total_mem.getAndAdd(memory);	
		
		System.out.println("Window " + window.id + " has " + count + 
				" results of avg length " + memory4results/count +
				" and max length " + max);
	}
}
