package transaction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import event.*;
import iogenerator.*;

public class BaseLine extends Transaction {	
	
	ArrayList<ArrayList<Event>> results;
	
	public BaseLine (Window w, OutputFileGenerator o, CountDownLatch tn, AtomicLong time, AtomicInteger mem) {		
		super(w,o,tn,time,mem);
		results = new ArrayList<ArrayList<Event>>();
	}
	
	public void run () {
		
		long start =  System.currentTimeMillis();
		computeResults();
		long end =  System.currentTimeMillis();
		long duration = end - start;
		total_cpu.set(total_cpu.get() + duration);
		
		writeOutput2File();		
		transaction_number.countDown();
	}
	
	public void computeResults() {						
				
		for (Event event: window.events) {		
			
			/*** CASE I: Create a new CET ***/
			if (results.isEmpty()) {			
				
				ArrayList<Event> new_result = new ArrayList<Event>();
				new_result.add(event);
				results.add(new_result);
				
			} else {	
				
				boolean added = false;
				ArrayList<ArrayList<Event>> new_results = new ArrayList<ArrayList<Event>>();
				
				for (ArrayList<Event> previous_result : results) {
					
					Event last = previous_result.get(previous_result.size()-1);
					Event before_last = (previous_result.size()-2>=0) ? previous_result.get(previous_result.size()-2) : null;
					
					/*** CASE II: Append to a CET ***/
					if (last.isCompatible(event)) {
						previous_result.add(event);
						added = true;
					} else {
						/*** CASE III: Append to a prefix of a CET ***/
						if (before_last!=null && before_last.isCompatible(event)) {
							ArrayList<Event> new_result = new ArrayList<Event>();
							new_result.addAll(previous_result);
							new_result.remove(previous_result.size()-1);
							new_result.add(event);
							if (!new_results.contains(new_result) && !results.contains(new_result)) new_results.add(new_result);
							added = true;
						}
					}
				}
				results.addAll(new_results);
				
				/*** CASE I: Create a new CET ***/
				if (!added) {
					ArrayList<Event> new_result = new ArrayList<Event>();
					new_result.add(event);
					results.add(new_result);					
				}						
			}					
		}		
	}	
	
	public void writeOutput2File() {
		
		int memory4results = 0;
		
		System.out.println("Window " + window.id + " has " + results.size() + " results.");
				
		//if (output.isAvailable()) {
			for(ArrayList<Event> trend : results) { 
				/*try { 
					for (Event event : trend) {
						output.file.append(event.id + ",");					
					}
					output.file.append("\n");
				} catch (IOException e) { e.printStackTrace(); }*/
				memory4results += trend.size();	
			}
			//output.setAvailable();
		//}			
		// Output of statistics
		total_mem.set(total_mem.get() + memory4results);
		//if (total_mem.get() < memory4results) total_mem.getAndAdd(memory4results);	
	}
}
