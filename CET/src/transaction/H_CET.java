package transaction;

import iogenerator.OutputFileGenerator;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Stack;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import event.*;
import graph.*;
import optimizer.*;

public class H_CET extends Transaction {
	
	Partitioning resulting_partitioning;
	double memory_limit;
	int cut_number;
	int search_algorithm;
	ArrayDeque<Window> windows;
	int window_slide;
	boolean overlap;
	SharedPartitions shared_partitions;
	ArrayList<String> results;
	
	public H_CET (Window w, OutputFileGenerator o, CountDownLatch tn, AtomicLong time, AtomicInteger mem, double ml, int pn, int sa, 
			ArrayDeque<Window> ws, int wsl, boolean overl, SharedPartitions sp) {
		super(w,o,tn,time,mem);	
		memory_limit = ml;
		cut_number = pn;
		search_algorithm = sa;
		windows = ws;
		window_slide = wsl;
		overlap = overl;
		shared_partitions = sp;
		results = new ArrayList<String>();
	}

	public void run() {	
		
		// long start =  System.currentTimeMillis();	
		
		// Size of the graph
		int size_of_the_graph = window.events.size();// + Graph.constructGraph(batch).edgeNumber;
		HashMap<Integer,Graph> graphlets = new HashMap<Integer,Graph>();
				
		if (search_algorithm<3) {
			Partitioner partitioner;
			if (search_algorithm==0) {
				partitioner = new Exh_topDown(windows);
			} else {
			if (search_algorithm==1) {
				partitioner = new BnB_topDown(windows);
			} else {
				partitioner = new Gre_topDown(windows);
			}}
			resulting_partitioning = partitioner.getPartitioning(window.events, memory_limit);
		} else {
			if (search_algorithm==3) {
				// Get an optimal partitioning with the given cut number
				resulting_partitioning = Partitioning.getOptimalPartitioning(window.events, cut_number);
			} else {
			if (search_algorithm==4) {
				
				// Get the partitioning with the given cut
				Partitioning max_partitioning = Partitioning.getPartitioningWithMaxPartition(window.events);
				ArrayList<Integer> cuts = new ArrayList<Integer>();
				cuts.add(cut_number);
				CutSet cutset = new CutSet(cuts);
				resulting_partitioning = max_partitioning.partitions.get(0).getPartitioning(cutset);
				System.out.println("Chosen: " + resulting_partitioning.toString(3));
								
			} else { // Get partitioning that shares graphlets between overlapping windows
				
				// Get partition identifiers of this window	
				ArrayList<String> partition_ids = new ArrayList<String>();
				int start = window.start;
				while (start+window_slide<window.end) { 
					String partition_id = start + " " + (start+window_slide-1);
					partition_ids.add(partition_id);
					start += window_slide;				
				}
				String partition_id = start + " " + window.end;
				partition_ids.add(partition_id);
				
				//System.out.println("Partitions: " + partition_ids.toString());
				
				// Write a new partition or read a stored partition
				ArrayList<Partition> parts = new ArrayList<Partition>();
				
				for (String pid : partition_ids) {	
					
					// Get start and end of the window
					String[] array = pid.split(" ");
					int s = Integer.parseInt(array[0]);
					int e = Integer.parseInt(array[1]);
										
					boolean writes = window.writes(s,overlap);
					if (writes) {
						
						// Select events from the batch
						ArrayList<Event> selected_events = new ArrayList<Event>();
						for (Event event : window.events) {
							if (event.sec >= s && event.sec <= e) selected_events.add(event);
							if (event.sec > e) break;
						}						
						// Construct a partition from these events
						Graph g = Graph.constructGraph(selected_events);						
						Partition part = new Partition(s,e,selected_events.size(),g.edgeNumber,g.first_nodes,g.last_nodes);
						parts.add(part);
						graphlets.put(s,g);
						//System.out.println("Graph written: " + part.id + " " + g.first_nodes.size());									
											
					} else {
						
						// Read a stored partition
						Partition part = shared_partitions.get(pid); 
						parts.add(part);
						//System.out.println("Graph read: " + part.id);
					}					
				}
				resulting_partitioning = new Partitioning(parts);
				//System.out.println("Resulting partitioning: " + resulting_partitioning.toString(3));				
		}}}
					
		if (!resulting_partitioning.partitions.isEmpty()) {
			
			long start =  System.currentTimeMillis();
			
			/*** Compute results within partitions ***/
			int cets_within_partitions = 0;
			for (Partition partition : resulting_partitioning.partitions) {	
			
				ArrayList<EventTrend> partitionResults = new ArrayList<EventTrend>();
				boolean writes = window.writes(partition.start,overlap);
				
				if (writes) {
					
					// If this window writes the results of this partition, compute these results
					for (Node first_node : partition.first_nodes) { first_node.isFirst = true; }
					partition.results = T_CET.computeResults(partition.last_nodes,writes,partitionResults);
					shared_partitions.add(partition.id, partition);
					cets_within_partitions += partition.getCETlength();
					//System.out.println("Results written: " + partition.id + " " + partition.results.size());
				} 			
			}
			/*** Draw edges between partitions ***/
			for (Partition partition : resulting_partitioning.partitions) {
				int prev_start = partition.start-window_slide;
				if (prev_start >=0) {
					
					int prev_end = (partition.start == window.end) ? window.end : (partition.start-1);
					String prev_pid = prev_start + " " + prev_end;	
					Partition prev_partition = shared_partitions.get(prev_pid);	
					
					while (prev_partition.first_nodes.isEmpty()) {
						//System.err.println(prev_pid + " is empty!!!");
						prev_start = prev_partition.start-window_slide;
						if (prev_start >=0) {							
							prev_end = (prev_partition.start == window.end) ? window.end : (prev_partition.start-1);
							prev_pid = prev_start + " " + prev_end;	
							prev_partition = shared_partitions.get(prev_pid);	
						}
					}
					
					for (Node first_node : partition.first_nodes)
						for (Node last_node : prev_partition.last_nodes)
							if (last_node.isCompatible(first_node))
								last_node.connect(first_node);
			}}		
			
			/*** Compute results across partitions ***/
			int max_cet_across_partitions = 0;
			Partition first = resulting_partitioning.partitions.get(0);
			for(int i=1; first.first_nodes.isEmpty(); i++){
				first = resulting_partitioning.partitions.get(i);
			}			
			for (Node first_node : first.first_nodes) {				
				for (EventTrend event_trend : first_node.results) {
					
					int length = computeResults(event_trend, new Stack<EventTrend>(), max_cet_across_partitions);				
					if (max_cet_across_partitions < length) max_cet_across_partitions = length;		
			}}
					
			long end =  System.currentTimeMillis();
			long duration = end - start;
			total_cpu.set(total_cpu.get() + duration);
		
			int memory = size_of_the_graph + cets_within_partitions + max_cet_across_partitions;
			writeOutput2File(memory);
		}
		transaction_number.countDown();		
	}
	
	// DFS recomputing intermediate results
	public int computeResults (EventTrend event_trend, Stack<EventTrend> current_cet, int maxSeqLength) {       
			
		current_cet.push(event_trend);
		//System.out.println("pushed " + event_trend.sequence);
	        
		/*** Base case: We hit the end of the graph. Output the current CET. ***/
	    if (event_trend.last_node.following.isEmpty()) {   
	       	String result = "";        	
	       	Iterator<EventTrend> iter = current_cet.iterator();
	       	int eventNumber = 0;
	       	while(iter.hasNext()) {
	       		EventTrend n = iter.next();
	       		result += n.sequence.toString() + ";";
	       		eventNumber += n.getEventNumber();
	       	}
	       	if (maxSeqLength < eventNumber) maxSeqLength = eventNumber;	
	       	String s = (!result.isEmpty()) ? result : event_trend.sequence;
	       	results.add(s);  
	       	//System.out.println("result " + s);
	   } else {
	   /*** Recursive case: Traverse the following nodes. ***/        	
	       	for(Node first_in_next_partition : event_trend.last_node.following) {   	       		
	       		for (EventTrend next_event_trend : first_in_next_partition.results) {       			
	       			maxSeqLength = computeResults(next_event_trend, current_cet, maxSeqLength);       			
	       		}	       		       		
	       	}        	
	   }
	   EventTrend top = current_cet.pop();
	   //System.out.println("popped " + top.sequence);
	   
	   return maxSeqLength;
   }
	
	public void writeOutput2File(int memory) {
		
		int maxSeqLength = 0;
		
		System.out.println("Window " + window.id + " has " + results.size() + " results.");
				
		if (output.isAvailable()) {					
				
			for(String sequence : results) {							 				
				try { output.file.append(sequence + "\n"); } catch (IOException e) { e.printStackTrace(); }
				int eventNumber = getEventNumber(sequence);
				if (maxSeqLength < eventNumber) maxSeqLength = eventNumber;			
			}
			output.setAvailable();
		}
		// Output of statistics
		total_mem.set(total_mem.get() + memory);
		//if (total_mem.get() < memory) total_mem.getAndAdd(memory);			
	}
	
}	