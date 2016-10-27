package event;

public class CheckEvent extends Event {
	
	int compatibility;
	
	public CheckEvent (int s, int i, int v) {
		super(s,i);
		compatibility = v;
	}
	
	public static Event parse (String line) {
		
		String[] values = line.split(",");
		
		int s = Integer.parseInt(values[0]);
        int i = Integer.parseInt(values[1]);
        int c = Integer.parseInt(values[2]);          	
    	    	    	
    	Event event = new CheckEvent(s,i,c);    	
    	//System.out.println(event.toString());    	
        return event;
	}

	public boolean isCompatible(Event other) {
		if (other instanceof CheckEvent) {
			CheckEvent o = (CheckEvent) other;
			return this.compatibility == o.compatibility && this.sec < o.sec;
		}
		return false;
	}
}
