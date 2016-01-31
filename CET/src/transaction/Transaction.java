package transaction;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import event.*;
import iogenerator.*;

public abstract class Transaction implements Runnable {
	
	ArrayList<Event> batch;		
	OutputFileGenerator output;
	public CountDownLatch transaction_number;
	long startOfSimulation;	
	
	public Transaction (ArrayList<Event> b, OutputFileGenerator o, CountDownLatch tn, long start) {		
		batch = b;			
		output = o; 
		transaction_number = tn;
		startOfSimulation = start;	
	}	
	
	
}
