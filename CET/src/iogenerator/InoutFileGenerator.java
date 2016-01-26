package iogenerator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Random;
import event.*;

public class InoutFileGenerator {
	
	/**
	 * INPUT PARAMETERS:
	 * @param args
	 * 0 output file
	 * 1 min_seq_number
	 * 2 max_seq_number
	 * 3 min_time_progress 
	 * 4 max_time_progress 
	 * 5 max_comp
	 */	
	public static void main (String [] args) {
		
		try { 
		
			// Open the output file
			String output_file_name = args[0];
			File output_file = new File(output_file_name);
			BufferedWriter output;
			output = new BufferedWriter(new FileWriter(output_file)); 		
		
			// Generate a random number of sequences
			Random random = new Random();
			int min_seq_number = Integer.parseInt(args[1]);
			int max_seq_number = Integer.parseInt(args[2]);
			int seq_number = random.nextInt(max_seq_number - min_seq_number + 1) + min_seq_number;
		
			// Local variables
			int sec = 0;
			int event_id = 0;
			int min_time_progress = Integer.parseInt(args[3]);
			int max_time_progress = Integer.parseInt(args[4]);
			int max_comp = Integer.parseInt(args[5]); 
			int comp = 0;
			
			// Output the parameters
			System.out.println("Seq number: " + seq_number +
					"\nMin time progress: " + min_time_progress +
					"\nMax time progress: " + max_time_progress +
					"\nMax compatibility: " + max_comp +
					"\n---------------------");
			
			// Generate sequences
			ArrayList<ArrayDeque<Event>> sequences = new ArrayList<ArrayDeque<Event>> ();
			
			for (int seq_id = 1; seq_id<=seq_number; seq_id++) { // seq_id 0 is invalid
				
				ArrayDeque<Event> sequence = new ArrayDeque<Event> ();
			
				// First event in a sequence
				sec = 0;
				Event e1 = new Event(event_id,sec,seq_id);	
				sequence.add(e1);
				System.out.println(e1.toString());
				event_id++;
				comp = random.nextInt(max_comp + 1);
				//System.out.println(comp);
			
				// All following events
				while (comp>0) {
					sec = sec + random.nextInt(max_time_progress - min_time_progress + 1) + min_time_progress;
					for (int i=0; i<comp; i++) {						
						Event e2 = new Event(event_id,sec,seq_id);
						sequence.add(e2);
						System.out.println(e2.toString());
						event_id++;
					}
					comp = random.nextInt(max_comp + 1);
					//System.out.println(comp);
				}	
				sequences.add(sequence);
			}
			// Put events in the file in order by time stamp
			int saved_events = 0;
			int curr_sec = 0;
			while (saved_events<event_id) {
				for (ArrayDeque<Event> sequence : sequences) {
					while (sequence.peek()!=null && sequence.peek().sec == curr_sec) {
						output.append(sequence.poll().print2file());
						saved_events++;
					}
				}
				curr_sec++;				
			}			
			// Close the file
			output.close();
			
		} catch (IOException e) { e.printStackTrace(); }
	}
}
