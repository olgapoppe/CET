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
		
		//HashSet<TreeSet<Event>> new_results = new HashSet<TreeSet<Event>>();
		HashSet<TreeSet<Event>> prefixes = new HashSet<TreeSet<Event>>();					
				
		for (Event event: window.events) {
			//System.out.println(node);
			prefixes = new HashSet<TreeSet<Event>>();
			/*** CASE I: Create a new CET ***/
			if (results.isEmpty()) {
				TreeSet<Event> newSeq = new TreeSet<Event>();
				newSeq.add(event);
				//System.out.println("seq on empty result : " + newSeq);
				//new_results.add(newSeq);	
				results.add(newSeq);
			} else {
				boolean isAdded=false;
				for (TreeSet<Event> seq : results) {
					TreeSet<Event> prefix = (TreeSet<Event>) seq.headSet(event); // !!!
					//System.out.println("prefix: "+prefix);
					//System.out.println("prefix-size : " + prefix.size());
					if (!prefix.isEmpty() && prefix.size() > 0) {
						isAdded=true;
						//System.out.println("added prefix: "+prefix);
						//System.out.println("from seq    : "+seq);
						/*** CASE II: Append to a CET ***/
						if(prefix.size()==seq.size()) {
							seq.add(event);
							//System.out.println("added to old : "+seq);
						} else {	
							/*** Duplicate elimination ***/
							TreeSet<Event> newSeq = (prefix.size()>0) ? (TreeSet<Event>)prefix.clone() : new TreeSet<Event>();
							//System.out.println("added new : "+ newSeq);								
							boolean duplicate=true;
							boolean unique=true;
							for(TreeSet<Event> oldPrefix : prefixes) {
								//System.out.println("before prefix : " + prefix);
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
										unique=false;
										break;
									}
								}
							}
							if(unique) prefixes.add(newSeq);								
						}
					}
				}
				//System.out.println("prefixes-size : " + prefixes.size());
				/*** CASE I: Create a new CET ***/
				if(prefixes.isEmpty()) {
					if(!isAdded) {
						TreeSet<Event> newSeq = new TreeSet<Event>();
						newSeq.add(event);
						//System.out.println("on empty newSeq : "+ newSeq);
						//new_results.add(newSeq);
						results.add(newSeq);
					}						
				} else {
					/*** CASE III: Append to a compatible CET ***/
					for(TreeSet<Event> prefix : prefixes) {
						//System.out.println("before prefix : " + prefix);
						prefix.add(event);
						//System.out.println("add from prefixes : " + prefix);
						//new_results.add(prefix);
						results.add(prefix);
						//System.out.println("results size : " + results.size());
					}
				}					
			}
			//System.out.println("results size : " + results.size());					
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
	
	/*public static void main (String args[]) {
	
	try {
		// Input
		String inputfile = "src\\iofiles\\stream.txt";
		Scanner scanner = new Scanner(new File(inputfile));		
		String line = scanner.nextLine();
		ArrayList<Event> batch = new ArrayList<Event>();
		Event event = Event.parse(line); 			
			while (event != null) { 				
				batch.add(event);
				if (scanner.hasNextLine()) {		 				
					line = scanner.nextLine();   
					event = Event.parse(line);		 				
				} else {
					event = null;		 				
				}
			}
			scanner.close(); 	
			// Output
			String outputfilename ="src\\iofiles\\sequences.txt";
			File outputfile = new File(outputfilename);
			BufferedWriter output = new BufferedWriter(new FileWriter(outputfile)); 
			// Call the method
			//get(batch,output);
	} catch (FileNotFoundException e) {	e.printStackTrace(); } 
	  catch (IOException e) { e.printStackTrace(); }		
	}

	public static void get(ArrayList<Event> batch, BufferedWriter output) {*/
}
