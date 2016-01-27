package transaction;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Scanner;
import java.util.TreeSet;
import event.*;

/** 
 * At the end of the window, the static base line algorithm computes all CETs.  
 * @author Olga Poppe
 */
public class StaticBaseLine { //extends Transaction {
	
	/*public StaticBaseLine (ArrayList<Event> batch, long startOfSimulation, PrintWriter out) {		
		super(batch, startOfSimulation, out);		
	}*/
	
	public static void main (String args[]) {
		
		try {
			String filename = "src\\iofiles\\stream.txt";
			Scanner scanner = new Scanner(new File(filename));		
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
 			get(batch);
		} catch (FileNotFoundException e) {	e.printStackTrace(); }		
	}
	
	public static void get(ArrayList<Event> batch) {	
		
		HashSet<TreeSet<Event>> results = new HashSet<TreeSet<Event>>();
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
		for(TreeSet<Event> path : results) {
			System.out.println(path);
			for (Event event : path) {
				System.out.print(event.id + ",");
			}
			System.out.println("\n-----------------------");
		}
		System.out.println("results size : " + results.size());
	}
}
