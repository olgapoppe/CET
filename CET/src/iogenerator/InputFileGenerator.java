package iogenerator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Random;
import event.*;

public class InputFileGenerator {
	
	/**
	 * INPUT PARAMETERS:
	 * @param args
	 * 0 output file
	 * 1 max_time_progress 
	 * 2 max_comp
	 * 3 last minute
	 */	
	public static void main (String [] args) {
		
		try { 		
			// Open the output file
			String output_file_name = args[0];
			File output_file = new File(output_file_name);
			BufferedWriter output;
			output = new BufferedWriter(new FileWriter(output_file)); 
			
			// Read input parameters
			int max_time_progress = Integer.parseInt(args[1]);
			int max_comp = Integer.parseInt(args[2]); 
			int last_min = Integer.parseInt(args[3]);
		
			// Local variables
			Random random = new Random();
			int last_sec = last_min * 60;
			int sec = 0;
			int event_id = 0;					
			int comp = 0;
			int value = 1; // value 0 is irrelevant
			int sequence_number = 0;
			
			// Output the parameters
			System.out.println(
					"Stream length: " + last_min + " min" +
					"\nMax time progress: " + max_time_progress +
					"\nMax compatibility: " + max_comp +
					"\n---------------------");
			
			//for (int init_sec = 0; init_sec+100<=end; init_sec+=99) {
				
				//int last_sec = init_sec + 100;
				//sec = init_sec;
			
			// Generate sequences
			ArrayList<ArrayDeque<Event>> all_events = new ArrayList<ArrayDeque<Event>> ();
			
			while (sec<=last_sec) {			
				 
				ArrayDeque<Event> events_with_same_value = new ArrayDeque<Event> ();
				int curr_sequence_number = 1;
			
				// First event in a sequence
				sec = random.nextInt(max_time_progress);
				value++;
				Event e1 = new Event(sec,event_id,value);	
				events_with_same_value.add(e1);
				//System.out.println(e1.toString());
				event_id++;
				comp = random.nextInt(max_comp + 1);
				//System.out.println(comp);
				if (comp>1) curr_sequence_number *= comp;
			
				// All following events
				while (comp>0) {
					sec = sec + random.nextInt(max_time_progress) + 1;
					if (sec>last_sec) break;
					for (int i=0; i<comp; i++) {						
						Event e2 = new Event(sec,event_id,value);
						events_with_same_value.add(e2);
						//System.out.println(e2.toString());
						event_id++;
					}
					comp = random.nextInt(max_comp + 1);
					//System.out.println(comp);
					if (comp>1) curr_sequence_number *= comp;
				}	
				all_events.add(events_with_same_value);
				sequence_number += curr_sequence_number;				
			}
			// Put events in the file in order by time stamp
			write2File(all_events,output,event_id,sequence_number);
		    //}
			
			// Close the file
			output.close();
			
		} catch (IOException e) { e.printStackTrace(); }
	}
	
	public static void write2File(ArrayList<ArrayDeque<Event>> all_events, BufferedWriter output, int event_id, int sequence_number) {
		
		int saved_events = 0;
		int curr_sec = 0;
		int event_rate = 0;
		int max_event_rate = 0;
		
		while (saved_events<event_id) {
			event_rate = 0;
			for (ArrayDeque<Event> events_with_same_value : all_events) {
				while (events_with_same_value.peek()!=null && events_with_same_value.peek().sec == curr_sec) {
					try { output.append(events_with_same_value.poll().print2file()); } catch (IOException e) { e.printStackTrace(); }
					saved_events++;
					event_rate++;
				}
			}
			if (max_event_rate < event_rate) max_event_rate = event_rate;
			curr_sec++;				
		}	
		System.out.println("---------------------" + 
				"\nSequence number: " + sequence_number +
				"\nEvent number: " + event_id +
				"\nEvent rate: " + max_event_rate);		
	}
}
