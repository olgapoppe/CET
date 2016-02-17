package transaction;

import iogenerator.OutputFileGenerator;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import event.*;
import optimizer.*;

public class Partitioned extends Transaction {
	
	Partitioning partitioning;
	
	public Partitioned (ArrayList<Event> b, OutputFileGenerator o, CountDownLatch tn, AtomicLong pT, AtomicInteger mMPW) {
		super(b,o,tn,pT,mMPW);			
	}

	public void run() {
		
		long start =  System.currentTimeMillis();
		partitioning = Partitioning.constructPartitioning(batch);
		//computeResults(graph.first_nodes);		
		long end =  System.currentTimeMillis();
		long processingDuration = end - start;
		processingTime.set(processingTime.get() + processingDuration);
		
		//writeOutput2File();
		transaction_number.countDown();
	}
}
