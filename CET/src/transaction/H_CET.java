package transaction;

import iogenerator.OutputFileGenerator;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
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
	Window window; 
	int window_slide;
	SharedPartitions shared_partitions;
	ArrayList<String> results;
	
	public H_CET (ArrayList<Event> b, OutputFileGenerator o, CountDownLatch tn, AtomicLong time, AtomicInteger mem, double ml, int pn, int sa, 
			ArrayDeque<Window> ws, Window w, int wsl, SharedPartitions sp) {
		super(b,o,tn,time,mem);	
		memory_limit = ml;
		cut_number = pn;
		search_algorithm = sa;
		windows = ws;
		window = w;
		window_slide = wsl;
		shared_partitions = sp;
		results = new ArrayList<String>();
	}

	public void run() {	
		
		long start =  System.currentTimeMillis();		
		
		// Size of the graph
		int size_of_the_graph = batch.size();// + Graph.constructGraph(batch).edgeNumber;
		
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
			resulting_partitioning = partitioner.getPartitioning(batch, memory_limit);
		} else {
			if (search_algorithm==3) {
				// Get an optimal partitioning with the given cut number
				resulting_partitioning = Partitioning.getOptimalPartitioning(batch, cut_number);
			} else {
			if (search_algorithm==4) {
				
				// Get the partitioning with the given cut
				Partitioning max_partitioning = Partitioning.getPartitioningWithMaxPartition(batch);
				ArrayList<Integer> cuts = new ArrayList<Integer>();
				cuts.add(cut_number);
				CutSet cutset = new CutSet(cuts);
				resulting_partitioning = max_partitioning.partitions.get(0).getPartitioning(cutset);
				System.out.println("Chosen: " + resulting_partitioning.toString(3));
								
			} else {
				
				Partitioning max_partitioning = Partitioning.getPartitioningWithMaxPartition(batch);
				
				// Get the partitioning of the first window depending on window overlap
				ArrayList<Integer> cuts = new ArrayList<Integer>();
				for (int cut=window_slide; cut+window.start<=window.end; cut+=window_slide) { cuts.add(cut); }		
					
				if (cuts.isEmpty()) {
					resulting_partitioning = max_partitioning;
					System.out.println("Chosen: " + resulting_partitioning.toString(2));						
				} else {
					CutSet cutset = new CutSet(cuts);
					resulting_partitioning = max_partitioning.partitions.get(0).getPartitioning(cutset);
					System.out.println("Chosen: " + resulting_partitioning.toString(3));
		}}}}
					
		if (!resulting_partitioning.partitions.isEmpty()) {
			
			//long start =  System.currentTimeMillis();
			
			/*** Compute results within partitions ***/
			int cets_within_partitions = 0;
			for (Partition partition : resulting_partitioning.partitions) {	
			
				ArrayList<EventTrend> partitionResults = new ArrayList<EventTrend>();
				boolean writes = window.writes(partition,windows);
				
				if (writes) {
					
					// If this window writes the results of this partition, compute these results
					for (Node first_node : partition.first_nodes) { first_node.isFirst = true; }
				
					partitionResults = T_CET.computeResults(partition.last_nodes,writes,partitionResults);
					shared_partitions.add(partition.id, partitionResults);
					System.out.println("Window " + window.id + " writes " + partitionResults.size() + " results for the partition " + partition.id);
				
					cets_within_partitions += partition.getCETlength();
				} else {
					
					// If this window reads the results of this partition, look these results up
					partitionResults = shared_partitions.get(partition.id);
					System.out.println("Window " + window.id + " reads " + partitionResults.size() + " results for the partition " + partition.id);					
				}				
			}		
		
			/*** Compute results across partitions ***/
			int max_cet_across_partitions = 0;
			for (Node first_node : resulting_partitioning.partitions.get(0).first_nodes) {
				
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
	       	// results.add(s);  
	       	// System.out.println("result " + result);
	   } else {
	   /*** Recursive case: Traverse the following nodes. ***/        	
	       	for(Node first_in_next_partition : event_trend.last_node.following) {        		
	       		// System.out.println("following of " + node.event.id + " is " + following.event.id);
	       		
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
	
	/*public void run() {	
		
		long start =  System.currentTimeMillis();
		
		*//*** Get the ideal memory in the middle of the search space 
		 * to decide from where to start the search: from the top or from the bottom ***//*
		int curr_sec = -1;
		int number_of_min_partitions = 0;
		for(Event event : batch) {
			if (curr_sec < event.sec) {
				curr_sec = event.sec;
				number_of_min_partitions++;
		}}		
		int event_number = batch.size();
		int edge_number = Graph.constructGraph(batch).edgeNumber;
		int size_of_the_graph = event_number + edge_number;
		double ideal_memory_in_the_middle = getIdealMEMcost(event_number, number_of_min_partitions/2, 3);
		boolean top_down = (memory_limit > ideal_memory_in_the_middle);		
		System.out.println("Top down: " + top_down);
		//System.out.println("Ideal memory in the middle: " + ideal_memory_in_the_middle);
		
		Partitioning input_partitioning;
		int algorithm = 0;
		int bin_number = 0;
		int bin_size = 0;
		Partitioner partitioner;		
		if (search_algorithm==1) { *//*** B&B ***//*
			
			input_partitioning = Partitioning.getPartitioningWithMaxPartition(batch);
			if (top_down) {
				partitioner = new BnB_topDown(windows);
				algorithm = 2;
			} else {
				partitioner = new BnB_bottomUp(windows,batch);
				algorithm = 3; // 1 or 3
			}			
		} else {
								 
			if (top_down) {
				bin_number = getMinNumberOfRequiredPartitions_walkDown(event_number,number_of_min_partitions,memory_limit);
			} else {
				bin_number = getMinNumberOfRequiredPartitions_walkUp(event_number,number_of_min_partitions,memory_limit);
			}
			bin_size = (bin_number==1) ? event_number : event_number/bin_number;
			System.out.println("Bin number: " + bin_number + "\nBin size: " + bin_size);
			
			if (search_algorithm==2) { *//*** Greedy partitioning search ***//*
				if (bin_size == 1) {
					input_partitioning = Partitioning.getPartitioningWithMaxPartition(batch);
					algorithm = 1;
				} else {
				if (bin_number == 1) {
					input_partitioning = Partitioning.getPartitioningWithMaxPartition(batch);
					algorithm = 2;	
				} else {
					input_partitioning = Partitioning.getPartitioningWithMinPartitions(batch);
					algorithm = 3; 
				}}			
				partitioner = new RandomRoughlyBalancedPartitioning(windows);
			
			} else { *//*** Exhaustive partitioning search ***//*
			
				input_partitioning = Partitioning.getPartitioningWithMaxPartition(batch);
				algorithm = 3;
				partitioner = new OptimalRoughlyBalancedPartitioning(windows);
		}}	
		System.out.println("Input: " + input_partitioning.toString(windows,algorithm));
		resulting_partitioning = partitioner.getPartitioning(input_partitioning, memory_limit, bin_number, bin_size);
		System.out.println("Result: " + resulting_partitioning.toString(windows,3)); // 1 or 3
		
		// The case where the 1st algorithm is called is missing
		
		if (!resulting_partitioning.partitions.isEmpty()) {
			
			long start =  System.currentTimeMillis();
			
			*//*** Compute results per partition ***//*
			int cets_within_partitions = 0;
			for (Partition partition : resulting_partitioning.partitions) {	
			
				//if (partition.isShared(windows)) System.out.println("Shared partition: " + partition.toString());
			
				for (Node first_node : partition.first_nodes) { first_node.isFirst = true; }
				T_CET.computeResults(partition.last_nodes);
				cets_within_partitions += partition.getCETlength();			
			}		
		
			*//*** Compute results across partition ***//*
			int max_cet_across_partitions = 0;
			for (Node first_node : resulting_partitioning.partitions.get(0).first_nodes) {
				
				for (EventTrend event_trend : first_node.results) {				
					int length = computeResults(event_trend, new Stack<EventTrend>(), max_cet_across_partitions);				
					if (max_cet_across_partitions < length) max_cet_across_partitions = length;		
			}}
		
			long end =  System.currentTimeMillis();
			long processingDuration = end - start;
			processingTime.set(processingTime.get() + processingDuration);
		
			int memory = size_of_the_graph + cets_within_partitions + max_cet_across_partitions;
			writeOutput2File(memory);
		}
		transaction_number.countDown();		
	}*/
}
