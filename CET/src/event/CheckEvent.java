package event;

public class CheckEvent extends Event {
	
	public CheckEvent (int s, int i, int v) {
		super(s,i,v);
	}
	
	public static Event parse (String line) {
		
		String[] values = line.split(",");
		
		int s = Integer.parseInt(values[0]);
        int i = Integer.parseInt(values[1]);
        int v = Integer.parseInt(values[2]);          	
    	    	    	
    	Event event = new CheckEvent(s,i,v);    	
    	//System.out.println(event.toString());    	
        return event;
	}

}
