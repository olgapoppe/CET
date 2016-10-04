package iogenerator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class OutputFileGenerator {
	
	public BufferedWriter file;
	public AtomicBoolean available;
	
	public OutputFileGenerator (String filename) {
		
		File output_file = new File(filename);
		try { file = new BufferedWriter(new FileWriter(output_file)); } catch (IOException e) { e.printStackTrace(); }
		available = new AtomicBoolean(true);
	}
	
	public synchronized void setAvailable () {		
		available.set(true);
		notifyAll();		
	}

	public synchronized boolean isAvailable () {		
		try {			
			while (!available.get()) {
				wait(); 						
			}	
		} catch (InterruptedException e) { e.printStackTrace(); }
		available.set(false);
		return true;		
	}
}
