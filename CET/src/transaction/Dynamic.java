package transaction;

import iogenerator.OutputFileGenerator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
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
		
		// Array for recursive call of this method
		ArrayList<Node> next_level_array = new ArrayList<Node>();
		// Hash for quick lookup of saved nodes
		HashMap<Integer,Integer> next_level_hash = new HashMap<Integer,Integer>();
		
		for (Node this_node : current_level) {
			// Base case: Create the results for the first nodes
			if (this_node.results.isEmpty()) this_node.results.add(this_node.toString()); 
			
			// Recursive case: Copy results from the current node to its following node and 
			// append this following node to each copied result 
			for (Node next_node : this_node.following) {
				for (String sequence : this_node.results) {					
					next_node.results.add(sequence + ";" + next_node.toString()); 
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
			Set<Integer> keys = graph.last_nodes.keySet();
			for (Integer key : keys) {
				ArrayList<Node> last_nodes = graph.last_nodes.get(key);
				for (Node last : last_nodes) {
					memory4results += last.printResults(output);
				}
			}
			output.setAvailable();
		}
		// Output of statistics
		int memory = graph.nodes.size() + graph.edgeNumber + memory4results;
		if (maxMemoryPerWindow.get() < memory) maxMemoryPerWindow.getAndAdd(memory);	
	}
}
