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
	int part_num;
	int search_algorithm;
	ArrayDeque<Window> windows;
	Window window; 
	SharedPartitions shared_trends;
	ArrayList<String> results;
	
	public H_CET (ArrayList<Event> b, OutputFileGenerator o, CountDownLatch tn, AtomicLong pT, AtomicInteger mMPW, double ml, int pn, int sa, ArrayDeque<Window> ws, Window w, SharedPartitions sp) {
		super(b,o,tn,pT,mMPW);	
		memory_limit = ml;
		part_num = pn;
		search_algorithm = sa;
		windows = ws;
		window = w;
		shared_trends = sp;
		results = new ArrayList<String>();
	}

	public void run() {	
		
		long start =  System.currentTimeMillis();
		
		//Partitioning input_partitioning = Partitioning.getPartitioningWithMaxPartition(batch);
		
		// Size of the graph
		/*int event_number = batch.size();
		int edge_number = input_partitioning.partitions.get(0).edgeNumber;
		int size_of_the_graph = event_number + edge_number;*/
		
		// Bin number
			
		int bin_number = getMinNumberOfRequiredPartitions_walkDown(batch,memory_limit);
		System.out.println("Bin number: " + bin_number);
				
		//Partitioner partitioner = (search_algorithm==0) ? new Exh_topDown(windows) : new BnB_topDown(windows);
									
		//System.out.println("Input: " + input_partitioning.toString(windows,2));
		//resulting_partitioning = partitioner.getPartitioning(input_partitioning, memory_limit);
		//System.out.println("Result: " + resulting_partitioning.toString(windows,3));*/ 
		
		// The case where the 1st algorithm is called is missing
		
		/*if (!resulting_partitioning.partitions.isEmpty()) {
			
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
			}}*/
		
			long end =  System.currentTimeMillis();
			long processingDuration = end - start;
			processingTime.set(processingTime.get() + processingDuration);
		
			/*int memory = size_of_the_graph + cets_within_partitions + max_cet_across_partitions;
			writeOutput2File(memory);
		}*/
		transaction_number.countDown();		
	}
	
	public double getIdealMEMcost (int event_number, int partition_number, int algorithm) {
		
		double exp;
		double ideal_memory;
		
		if (algorithm == 1) {
			ideal_memory = event_number;
		} else {
		if (algorithm == 2) {
			ideal_memory = Math.pow(3, event_number) * event_number;			
		} else {
			double vertex_number_per_partition = event_number/new Double(partition_number);
			exp = vertex_number_per_partition/new Double(3);			
			ideal_memory = partition_number * Math.pow(3, exp) * vertex_number_per_partition + event_number;
		}}
		return ideal_memory;
	}
	
	/*** Get minimal number of required partitions walking the search space top down ***/
	public int getMinNumberOfRequiredPartitions_walkDown(ArrayList<Event> batch, double memory_limit) {	
		
		int event_number = batch.size();
		
		// Find the number of minimal partitions
		int s = 1;
		int e = 0;
		int curr_sec = -1;		
		for(Event event : batch) {
			if (curr_sec < event.sec) {
				curr_sec = event.sec;
				e++;
		}}
		
		// Find the minimal number of required partitions (H-CET)
		int m = 0;
		double ideal_memory = 0;
		int level = 0;
		while (s <= e) {	
			m = s + (e-s)/2;
			ideal_memory = getIdealMEMcost(event_number,m,3);						
			System.out.println("k=" + m + " mem=" + ideal_memory);
			
			if (ideal_memory <= memory_limit) {
				level = m;
				e = m - 1;
			} else {
				s = m + 1;
			}
			System.out.println("s=" + s + " e=" + e + "\n");
		}	
		return (level > 0) ? level : event_number;
	}
	
	/*** Get minimal number of required partitions walking the search space bottom up ***/
	public int getMinNumberOfRequiredPartitions_walkUp(int event_number, int number_of_min_partitions, double memory_limit) {	
		
		// Partitioning does not reduce the memory enough zz
		int result = -1;
		
		// Each event is in a separate partition (M-CET)
		if (event_number <= memory_limit) result = event_number;
		
		// Find the minimal number of required partitions (T-CET, H-CET)
		for (int partition_number=number_of_min_partitions; partition_number>0; partition_number--) {
			double ideal_memory = getIdealMEMcost(event_number,partition_number,3);
			if (ideal_memory <= memory_limit) {
				result = partition_number;
				
				System.out.println("k=" + partition_number + " mem=" + ideal_memory);
			} else {
				break;
			}
		}				
		return result;
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
		if (maxMemoryPerWindow.get() < memory) maxMemoryPerWindow.getAndAdd(memory);			
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
