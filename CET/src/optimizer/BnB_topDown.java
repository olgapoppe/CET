package optimizer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import event.*;
import graph.*;

public class BnB_topDown extends Partitioner {	
	
	public BnB_topDown (ArrayDeque<Window> w) {
		super(w);
	}
	
	public Partitioning getPartitioning (ArrayList<Event> batch, double memory_limit) {
		
		// Set local variables
		LinkedList<Partitioning> heap = new LinkedList<Partitioning>();
		Partitioning solution = new Partitioning(new ArrayList<Partition>());
		HashMap<String,Integer> pruned = new HashMap<String,Integer>();
				
		double minCPU = Double.MAX_VALUE;		
		int maxHeapSize = 0;
		int considered_count = 0;
		
		/*** Level search ***/			
		int level = getMinNumberOfRequiredPartitions_walkDown(batch,memory_limit);
		System.out.println("Min number of required partitions: " + level);
		
		/*** Node search ***/
		//ArrayList<Partitioning> nodes = getNodesAtLevel(batch,level);		
		//heap.addAll(nodes);			
		
		while (!heap.isEmpty()) {
			
			// Get the next node to process, its costs and children 
			Partitioning temp = heap.poll();			
			if (pruned.containsKey(temp.id)) continue;
			double temp_cpu = temp.getCPUcost(windows, 3);
			double temp_mem = temp.getMEMcost(windows, 3);
			
			ArrayList<Partitioning> children = temp.getChildrenBySplitting();
			
			//System.out.println("Considered: " + temp.toString());
			
			considered_count++;
			
			if (temp_mem > memory_limit) {
				
				// Add children to the heap				
				for (Partitioning child : children) {					
					if (!heap.contains(child) && !pruned.containsKey(child.id)) 
						heap.add(child); 
				} 
				// Update max heap size
				if (maxHeapSize < heap.size()) 
					maxHeapSize = heap.size();
			} else {
				// Update solution
				if (temp_cpu < minCPU) {
					solution = temp;
					minCPU = temp_cpu;
				}
				// Prune the children
				for (Partitioning child : children) {
					pruned.put(child.id, 1);
				}
			}			
		}
		System.out.println("Max heap size: " + maxHeapSize + 
				"\nConsidered: " + considered_count);	
		
		//System.out.println("Chosen: " + solution.toString()); 
		
		return solution;		
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
		
		// Find the minimal number of required partitions
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
	
	static void combinationUtil(int arr[], int data[], int start, int end, int index, int r) {
		// Current combination is ready to be printed, print it
		if (index == r) {
			for (int j=0; j<r; j++)
				System.out.print(data[j]+" ");
			System.out.println("");
			return;
		}

		// replace index with all possible elements. The condition
		// "end-i+1 >= r-index" makes sure that including one element
		// at index will make a combination with remaining elements
		// at remaining positions
		for (int i=start; i<=end && end-i+1 >= r-index; i++) {
			data[index] = arr[i];
			combinationUtil(arr, data, i+1, end, index+1, r);
		}
	}

	// The main function that prints all combinations of size r
	// in arr[] of size n. This function mainly uses combinationUtil()
	static void printCombination(int arr[], int n, int r) {
		// A temporary array to store all combination one by one
		int data[]=new int[r];

		// Print all combination using temprary array 'data[]'
		combinationUtil(arr, data, 0, n-1, 0, r);
	}

	/*Driver function to check for above function*/
	public static void main (String[] args) {
		int arr[] = {1, 2, 3, 4, 5};
		int r = 5;
		int n = arr.length;
		printCombination(arr, n, r);
	}
}
