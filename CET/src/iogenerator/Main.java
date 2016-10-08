package iogenerator;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import event.*;
import scheduler.*;
 
public class Main {
	
	/**
	 * Example parameters: 
	 * -type check -path src/iofiles/ -file stream.txt -to 120 -algo tcet
	 * -type activity -path ../../../Dropbox/DataSets/PhysicalActivity/ -file all.dat -to 60 -algo tcet
	 * -type stock -path ../../../Dropbox/DataSets/Stock/ -file sorted.txt -to 60 -algo tcet
	 */
	public static void main (String[] args) { 
		
		try {
		
		/*** Print current time ***/
		Date dNow = new Date( );
	    SimpleDateFormat ft = new SimpleDateFormat ("E yyyy.MM.dd 'at' hh:mm:ss a zzz");
	    System.out.println("----------------------------------\nCurrent Date: " + ft.format(dNow));
	    
	    /* Path currentRelativePath = Paths.get("");
	    String s = currentRelativePath.toAbsolutePath().toString();
	    System.out.println("Current relative path is: " + s); */
	    
	    /*** Input and output ***/
	    // Set default values
	    String path = "iofiles/";
		String inputfile = "stream1.txt";
		String outputfile = "sequences.txt";		
		String type = "stock";
		
		boolean realtime = false;
		boolean overlap = false;
		int firstsec = 0;
	    int lastsec = 0;
		int window_length = 0;
		int window_slide = 0;	
		String algorithm = "";
		double memory_limit = Double.MAX_VALUE;
		int cut_number = -1;
		int search_algorithm = 0;
				
		// Read input parameters
	    for (int i=0; i<args.length; i++){
	    	if (args[i].equals("-type")) 		type = args[++i];
			if (args[i].equals("-path")) 		path = args[++i];
			if (args[i].equals("-file")) 		inputfile = args[++i];
			if (args[i].equals("-realtime")) 	realtime = Integer.parseInt(args[++i]) == 1;
			if (args[i].equals("-overlap")) 	overlap = Integer.parseInt(args[++i]) == 1;
			if (args[i].equals("-from")) 		firstsec = Integer.parseInt(args[++i]);
			if (args[i].equals("-to")) 			lastsec = Integer.parseInt(args[++i]);
			if (args[i].equals("-wl")) 			window_length = Integer.parseInt(args[++i]);
			if (args[i].equals("-ws")) 			window_slide = Integer.parseInt(args[++i]);
			if (args[i].equals("-algo")) 		algorithm = args[++i];
			if (args[i].equals("-mem")) 		memory_limit = Double.parseDouble(args[++i]);
			if (args[i].equals("-cut")) 		cut_number = Integer.parseInt(args[++i]);
			if (args[i].equals("-search")) 		search_algorithm = Integer.parseInt(args[++i]);
		}
	    String input = path + inputfile;
	    OutputFileGenerator output = new OutputFileGenerator(path+outputfile); 
	    if (window_length == 0 && window_slide == 0) {
	    	window_length = lastsec+1;
	    	window_slide = lastsec+1;
	    }
	    
	    // Print input parameters
	    System.out.println(	"Algorithm: " + algorithm +
	    					"\nInput file: " + inputfile +
	    					"\nType: " + type +
	    					"\nReal time: " + realtime +
	    					"\nOverlapping window: " + overlap +
	    					"\nStream from " + firstsec + " to " + lastsec +
	    					"\nWindow length: " + window_length + 
							"\nWindow slide: " + window_slide +
							"\nMemory limit: " + memory_limit +
							"\nCut number: " + cut_number +
							"\nSearch algorithm: " + search_algorithm +
							"\n----------------------------------");

		/*** SHARED DATA STRUCTURES ***/		
		AtomicInteger driverProgress = new AtomicInteger(-1);	
		EventQueue eventqueue = new EventQueue(driverProgress);						
		CountDownLatch done = new CountDownLatch(1);
		long startOfSimulation = System.currentTimeMillis();
		AtomicInteger eventNumber = new AtomicInteger(0);
		AtomicLong total_cpu = new AtomicLong(0);	
		AtomicInteger total_memory = new AtomicInteger(0);
		
		/*** EXECUTORS ***/
		int window_number = (lastsec-firstsec)/window_slide + 1;
		ExecutorService executor = Executors.newFixedThreadPool(10);
			
		/*** Create and start the event driver and the scheduler threads.
		 *   Driver reads from the file and writes into the event queue.
		 *   Scheduler reads from the event queue and submits event batches to the executor. ***/
		EventDriver driver = new EventDriver (input, type, realtime, lastsec, eventqueue, startOfSimulation, driverProgress, eventNumber);				
				
		Scheduler scheduler = new Scheduler (eventqueue, firstsec, lastsec, window_length, window_slide, algorithm, memory_limit, cut_number, search_algorithm, 
				executor, driverProgress, done, total_cpu, total_memory, output, overlap);		
		
		Thread prodThread = new Thread(driver);
		prodThread.setPriority(10);
		prodThread.start();
		
		Thread consThread = new Thread(scheduler);
		consThread.setPriority(10);
		consThread.start();		
				
		/*** Wait till all input events are processed and terminate the executor ***/
		done.await();		
		executor.shutdown();	
		output.file.close();
		
		System.out.println(	"\nAvg CPU: " + total_cpu.get()/window_number +				
							"\nAvg MEM: " + total_memory.get()/window_number + "\n");
				
		} catch (InterruptedException e) { e.printStackTrace(); }
		  catch (IOException e1) { e1.printStackTrace(); }
	}	
}