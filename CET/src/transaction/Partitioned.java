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
	int memory_limit;
	
	public Partitioned (ArrayList<Event> b, OutputFileGenerator o, CountDownLatch tn, AtomicLong pT, AtomicInteger mMPW, int ml) {
		super(b,o,tn,pT,mMPW);	
		memory_limit = ml;
	}

	public void run() {
		
		long start =  System.currentTimeMillis();
		BranchAndBound bab = new BranchAndBound();
		Partitioning rootPartitioning = Partitioning.constructRootPartitioning(batch);		
		partitioning = bab.getPartitioning(rootPartitioning, memory_limit);
		//computeResults(graph.first_nodes);		
		long end =  System.currentTimeMillis();
		long processingDuration = end - start;
		processingTime.set(processingTime.get() + processingDuration);
		
		//writeOutput2File();
		transaction_number.countDown();
	}
}
