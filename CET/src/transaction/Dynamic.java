package transaction;

import iogenerator.OutputFileGenerator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import event.Event;
import graph.Graph;
import graph.Node;

public class Dynamic extends Transaction {
	
	Graph graph;
	
	public Dynamic (ArrayList<Event> b, OutputFileGenerator o, CountDownLatch tn, AtomicLong pT,AtomicInteger mMPW) {
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
	public void computeResults (ArrayList<Node> current_level) { 
		
		ArrayList<Node> next_level = new ArrayList<Node>();
		for (Node this_node : current_level) {
			// Base case: Create the results for the first nodes
			if (this_node.results.isEmpty()) {
				ArrayList<Node> result = new ArrayList<Node>();
				result.add(this_node);
				this_node.results.add(result);
			}
			// Recursive case: Copy results from the current node to its following node and 
			// append this following node to each copied result 
			for (Node next_node : this_node.following) {
				for (ArrayList<Node> result : this_node.results) {
					ArrayList<Node> new_result = new ArrayList<Node>();
					new_result.addAll(result);
					new_result.add(next_node);
					next_node.results.add(new_result);	
				}	
				// Check that following is not in next_level
				if (!next_level.contains(next_node)) next_level.add(next_node); 
			}
			// Free data structures
			ArrayList<Node> last_nodes = graph.last_nodes.get(this_node.event.value);
			if (!last_nodes.contains(this_node)) this_node.results.clear();			
		}		
		// Call this method recursively
		if (!next_level.isEmpty()) computeResults(next_level);
	}
	
	public void writeOutput2File() {
		
		int memory4results = 0;
		/*int min = Integer.MAX_VALUE;
		int max = Integer.MIN_VALUE;
		int sequence_count = 0;*/		
		
		//try {	
			if (output.isAvailable()) {
				Set<Integer> keys = graph.last_nodes.keySet();
				for (Integer key : keys) {
					ArrayList<Node> last_nodes = graph.last_nodes.get(key);
					for (Node last : last_nodes) {
						memory4results += last.printResults(output);
						/*for(ArrayList<Node> sequence : last.results) { 							
							//System.out.println(sequence.toString());							
							for (Node node : sequence) {
								//System.out.print(event.id + ",");
								if (min > node.event.sec) min = node.event.sec;
								if (max < node.event.sec) max = node.event.sec;
								output.file.append(node.event.print2fileInASeq());
							}
							//System.out.println("\n-----------------------");
							output.file.append("\n");
						}
						sequence_count += last.results.size();*/
					}
				}
				output.setAvailable();
			}
		//} catch (IOException e) { e.printStackTrace(); }
			
		// Output of statistics
		int memory = graph.nodes.size() + graph.edgeNumber + memory4results;
		if (maxMemoryPerWindow.get() < memory) maxMemoryPerWindow.getAndAdd(memory);
		
		//System.out.println("Current max memory: " + memory);
		//if (sequence_count>0) System.out.println("Number of sequences: " + sequence_count + " Min: " + min + " Max: " + max);
	}
}
