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
	
	HashSet<TreeSet<Event>> results;
	
	public BaseLine (Window w, OutputFileGenerator o, CountDownLatch tn, AtomicLong time, AtomicInteger mem) {		
		super(w,o,tn,time,mem);
		results = new HashSet<TreeSet<Event>>();
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
		
		HashSet<TreeSet<Event>> prefixes = new HashSet<TreeSet<Event>>();					
				
		for (Event event: window.events) {
			
			prefixes = new HashSet<TreeSet<Event>>();
			
			/*** CASE I: Create a new CET ***/
			if (results.isEmpty()) {
				
				TreeSet<Event> newSeq = new TreeSet<Event>();
				newSeq.add(event);		
				results.add(newSeq);
				
				System.out.println("new CET : " + newSeq);				
				
			} else {
				boolean isAdded=false;
				for (TreeSet<Event> seq : results) {
					
					TreeSet<Event> prefix = (TreeSet<Event>) seq.headSet(event); // !!!
					
					if (!prefix.isEmpty() && prefix.size() > 0) {
						isAdded=true;
						
						/*** CASE II: Append to a CET ***/
						if(prefix.size()==seq.size()) {
							seq.add(event);
							
							System.out.println("added to CET : " + seq);
							
						} else {	
							/*** Duplicate elimination ***/
							TreeSet<Event> newSeq = (prefix.size()>0) ? (TreeSet<Event>)prefix.clone() : new TreeSet<Event>();
							boolean duplicate=true;
							
							for(TreeSet<Event> oldPrefix : prefixes) {
								
								if(oldPrefix.size()==newSeq.size()) {
									
									Iterator<Event> oldIterator=oldPrefix.iterator();
									Iterator<Event> newIterator=newSeq.iterator();
									
									duplicate=true;
									
									while(oldIterator.hasNext()) {
										if(!(oldIterator.next().equals(newIterator.next()))) {
											//System.out.println("duplicate prefix : " + newSeq);
											duplicate=false;
											break;												
										}
									}
									if(duplicate) {
										
										break;
									}
								}
							}
							if(!duplicate) prefixes.add(newSeq);								
						}
					}
				}
				
				/*** CASE I: Create a new CET ***/
				if(prefixes.isEmpty()) {
					if(!isAdded) {
						
						TreeSet<Event> newSeq = new TreeSet<Event>();
						newSeq.add(event);
						results.add(newSeq);
						
						System.out.println("new CET : " + newSeq);						
					}						
				} else {
					/*** CASE III: Append to a compatible CET ***/
					for(TreeSet<Event> prefix : prefixes) {
						
						prefix.add(event);
						results.add(prefix);
						
						System.out.println("added to prefix : " + prefix);
											
					}
				}					
			}					
		}		
	}	
	
	public void writeOutput2File() {
		
		int memory4results = 0;
		
		System.out.println("Window " + window.id + " has " + results.size() + " results.");
				
		//if (output.isAvailable()) {
			for(TreeSet<Event> sequence : results) { 
				/*try { 
					for (Event event : sequence) {
						output.file.append(event.id + ",");					
					}
					output.file.append("\n");
				} catch (IOException e) { e.printStackTrace(); }*/
				memory4results += sequence.size();	
			}
			//output.setAvailable();
		//}			
		// Output of statistics
		total_mem.set(total_mem.get() + memory4results);
		//if (total_mem.get() < memory4results) total_mem.getAndAdd(memory4results);	
	}
}
