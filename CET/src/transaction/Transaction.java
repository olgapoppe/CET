package transaction;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import event.*;
import iogenerator.*;

/** 
 * A transaction has an event batch, start of simulation and transaction number. 
 * @author Olga Poppe
 */
public abstract class Transaction implements Runnable {
	
	ArrayList<Event> batch;	
	long startOfSimulation;
	public CountDownLatch transaction_number;
	OutputFileGenerator output;
	
	public Transaction (ArrayList<Event> b, long start, CountDownLatch tn, OutputFileGenerator o) {		
		batch = b;		
		startOfSimulation = start;	
		transaction_number = tn;
		output = o; 
	}	
}
