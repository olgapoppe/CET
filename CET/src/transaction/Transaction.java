package transaction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import event.*;

/** 
 * A transaction has an event batch, start of simulation and transaction number. 
 * @author Olga Poppe
 */
public abstract class Transaction implements Runnable {
	
	ArrayList<Event> batch;	
	long startOfSimulation;
	public CountDownLatch transaction_number;
	BufferedWriter output;
	
	public Transaction (ArrayList<Event> b, long start, CountDownLatch tn) {		
		batch = b;		
		startOfSimulation = start;	
		transaction_number = tn;
		String outputfilename ="src\\iofiles\\sequences.txt";
		File outputfile = new File(outputfilename);
		try { output = new BufferedWriter(new FileWriter(outputfile)); } catch (IOException e) { e.printStackTrace(); } 
	}	

}
