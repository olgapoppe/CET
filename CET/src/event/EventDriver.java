package event;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

public class EventDriver implements Runnable {	
	
	String filename;
	String type;
	boolean realtime;
	int lastsec;
	final EventQueue eventqueue;			
	long startOfSimulation;
	AtomicInteger drProgress;
	AtomicInteger eventNumber;
		
	public EventDriver (String f, String t, boolean rt, int last, EventQueue eq, long start, AtomicInteger dp, AtomicInteger eN) {
		
		type = t;
		filename = f;
		realtime = rt;
		lastsec = last;
		eventqueue = eq;			
		startOfSimulation = start;
		drProgress = dp;
		eventNumber = eN;
	}

	/** 
	 * Read the input file, parse the events, 
	 * and put events into the event queue in timely manner.
	 */
	public void run() {	
		try {
			// Local variables
			double system_time = 0;
			double driver_wakeup_time = 0;
			// Input file
			Scanner scanner = new Scanner(new File(filename));
			// First event
			String line = scanner.nextLine();
	 		Event event = Event.parse(line,type);
	 		// Current Second
	 		int curr_sec = -1;		
			// First batch			
			Random random = new Random();
			int min = 6;
			int max = 14;			
			int end = random.nextInt(max - min + 1) + min;
			Window batch = new Window(0,end);
									
 			if (batch.end > lastsec) batch.end = lastsec;	
 			//System.out.println("\n-------------------------\nBatch end: " + batch.end);
 			
 			/*** Put events within the current batch into the event queue ***/		
	 		while (true) { 
	 		
	 			while (event != null && event.sec <= batch.end) {	 			
	 				
	 				/*** Put the event into the event queue and increment the counter ***/						
	 				eventqueue.contents.add(event);	
	 				eventNumber.set(eventNumber.get()+1);
	 					 					
	 				/*** Set distributer progress ***/	
	 				if (curr_sec < event.sec) {		
	 					
	 				// Avoid null run exception when the stream is read too fast
	 					if (curr_sec>300) { 
	 						eventqueue.setDriverProgress(curr_sec);
	 						//if (curr_sec % 10 == 0) System.out.println("Distribution time of second " + curr_sec + " is " + now);
	 					}
	 					curr_sec = event.sec;
	 				}
	 			
	 				/*** Reset event ***/
	 				if (scanner.hasNextLine()) {		 				
	 					line = scanner.nextLine();   
	 					event = Event.parse(line,type);		 				
	 				} else {
	 					event = null;		 				
	 				}
	 			}		 			
	 			/*** Set distributor progress ***/		 					
	 			eventqueue.setDriverProgress(batch.end);					
	 			curr_sec = batch.end;
	 				 			
				if (batch.end < lastsec) { 			
 				
					/*** Sleep if now is smaller than batch_limit ms ***/
					system_time = System.currentTimeMillis() - startOfSimulation;
					//System.out.println("Skipped time is " + skipped_time + " sec.\nSystem time is " + system_time/1000);
					
					if (realtime && system_time < batch.end*1000) { // !!!
	 			
						int sleep_time = new Double(batch.end*1000 - system_time).intValue(); // !!!	 			
						//System.out.println("Distributor sleeps " + sleep_time + " ms at " + curr_sec );		 			
						try { Thread.sleep(sleep_time); } catch (InterruptedException e) { e.printStackTrace(); }
						driver_wakeup_time = (System.currentTimeMillis() - startOfSimulation)/1000 - batch.end; // !!!
					} 
					
					/*** Reset batch_limit ***/
					int new_start = batch.end + 1;
					int new_end = batch.end + random.nextInt(max - min + 1) + min + new Double(driver_wakeup_time).intValue();
					batch = new Window(new_start, new_end);
					if (batch.end > lastsec) batch.end = lastsec;
					//System.out.println("-------------------------\nBatch end: " + batch.end);
 				
					if (driver_wakeup_time > 1) {
						System.out.println(	"Distributor wakeup time is " + driver_wakeup_time + 
											". New batch is " + batch.toString() + ".");
					}	 				
				} else { /*** Terminate ***/	 				
					break;
				}						
	 		}
	 		
	 		/*** Clean-up ***/		
			scanner.close();				
			//System.out.println("Driver is done.");	
 		
		} catch (FileNotFoundException e) { e.printStackTrace(); }
	}	
}
