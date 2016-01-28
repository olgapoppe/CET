package transaction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Scanner;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

import event.*;

/** 
 * At the end of the window, the static base line algorithm computes all CETs.  
 * @author Olga Poppe
 */
public class BaseLine extends Transaction {
	
	HashSet<TreeSet<Event>> results;
	
	public BaseLine (ArrayList<Event> batch, long startOfSimulation, CountDownLatch transaction_number, HashSet<TreeSet<Event>> r) {		
		super(batch, startOfSimulation, transaction_number);	
		results = r;
	}
	
	public static void main (String args[]) {
		
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
	
	//public static void get(ArrayList<Event> batch, BufferedWriter output) {
	
	public void run () {
		
		//HashSet<TreeSet<Event>> results = new HashSet<TreeSet<Event>>();
		HashSet<TreeSet<Event>> prefixes = new HashSet<TreeSet<Event>>();
			
		for (Event event: batch) {
			//System.out.println(node);
			prefixes = new HashSet<TreeSet<Event>>();
			/*** CASE I: Create a new CET ***/
			if (results.isEmpty()) {
				TreeSet<Event> newSeq = new TreeSet<Event>();
				newSeq.add(event);
				//System.out.println("seq on empty result : " + newSeq);
				results.add(newSeq);					
			} else {
				boolean isAdded=false;
				for (TreeSet<Event> seq : results) {
					TreeSet<Event> prefix = (TreeSet<Event>) seq.headSet(event);
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
						results.add(newSeq);
					}						
				} else {
					/*** CASE III: Append to a compatible CET ***/
					for(TreeSet<Event> prefix : prefixes) {
						//System.out.println("before prefix : " + prefix);
						prefix.add(event);
						//System.out.println("add from prefixes : " + prefix);
						results.add(prefix);
						//System.out.println("results size : " + results.size());
					}
				}					
			}
			//System.out.println("results size : " + results.size());					
		}
		/*** Write sequences to file ***/
		int min = Integer.MAX_VALUE;
		int max = Integer.MIN_VALUE;
		try {			
			for(TreeSet<Event> sequence : results){
				//System.out.println(sequence);
				for (Event event : sequence) {
					//System.out.print(event.id + ",");
					if (min > event.sec) min = event.sec;
					if (max < event.sec) max = event.sec;
					output.append(event.print2fileInASeq());
				}
				//System.out.println("\n-----------------------");
				output.append("\n");
			}
			output.close();
		} catch (IOException e) { e.printStackTrace(); }
		if (!results.isEmpty()) System.out.println("Number of sequences: " + results.size() + " Min: " + min + " Max: " + max);
		transaction_number.countDown();
	}
}
