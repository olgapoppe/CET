package iogenerator;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import event.*;
 
public class Main {
	
	/**
	 * Create and call the chain: Input file -> Driver -> Executor -> Output files
	 * 
	 * @param args: 
	 */
	public static void main (String[] args) { 
		
		/*** Print current time to know when the experiment started ***/
		Date dNow = new Date( );
	    SimpleDateFormat ft = new SimpleDateFormat ("E yyyy.MM.dd 'at' hh:mm:ss a zzz");
	    System.out.println("Current Date: " + ft.format(dNow));
	    
	    /*** INPUT ***/
	    // Set default values
	    String path = "\\src\\input_output\\";
		String filename = new String("inputfile.txt");
	    int event_rate = 10;
	    int lastsec = 10;
		int compatibility = 2; // max number of following compatible events
		int window_length = 5;
		int window_overlap_size = 0;		
		
		// Read input parameters
	    for (int i=0; i< args.length; i++){
			if (args[i].equals("-path")) 		path = args[++i];
			if (args[i].equals("-filename")) 	filename = args[++i];
			if (args[i].equals("-rate")) 		event_rate = Integer.parseInt(args[++i]);
			if (args[i].equals("-sec")) 		lastsec = Integer.parseInt(args[++i]);
			if (args[i].equals("-comp")) 		compatibility = Integer.parseInt(args[++i]);
			if (args[i].equals("-wl")) 			window_length = Integer.parseInt(args[++i]);
			if (args[i].equals("-wos")) 		window_overlap_size = Integer.parseInt(args[++i]);
		}
	    String full_file_name = path + filename;
	    
	    // Print input parameters
	    System.out.println(	"Input file: " + full_file_name +
	    					"\nEvent rate: " + event_rate +
	    					"\nLast sec: " + lastsec +
	    					"\nCompatibility: " + compatibility +
	    					"\nWindow length: " + window_length + 
							"\nWindow overlap length: " + window_overlap_size);

		/*** SHARED DATA STRUCTURES ***/		
		AtomicInteger driverProgress = new AtomicInteger(-1);	
		EventQueue eventqueue = new EventQueue(driverProgress);
						
		CountDownLatch done = new CountDownLatch(1);
		long startOfSimulation = System.currentTimeMillis();	
		
		/*** EXECUTORS ***/
		int number_of_executors = Integer.parseInt(args[0]);
		//System.out.println("Number of executors: " + number_of_executors);
		ExecutorService executor = Executors.newFixedThreadPool(number_of_executors);
			
		/*** Create and start event distributing and query scheduling THREADS.
		 *   Driver reads from the file and writes into runs and event queues.
		 *   Scheduler reads from runs and run queues and submits tasks to executor. ***/
		EventDriver driver = new EventDriver (filename, lastsec, eventqueue, startOfSimulation, driverProgress);				
				
		/*Scheduler scheduler = new TimeDrivenScheduler(
				max_xway, both_dirs, lastSec,
				runs, eventqueues, executor, 
				distributorProgress, distrFinishTimes, schedStartTimes, transaction_number, done, 
				startOfSimulation, optimized, total_exe_time, query_number, expensive_windows);*/		
		
		Thread prodThread = new Thread(driver);
		prodThread.setPriority(10);
		prodThread.start();
		
		/*Thread consThread = new Thread(scheduler);
		consThread.setPriority(10);
		consThread.start();*/
		
		try {			
			/*** Wait till all input events are processed and terminate the executor ***/
			done.await();		
			executor.shutdown();	
			System.out.println("Executor is done.");
			System.out.println("Main is done.");
			
		} catch (InterruptedException e) { e.printStackTrace(); }
	}	
}