/**
 *
 * @author Ke Wang Since June 2011
 */

/*
 * this is the class of event, implementing the comparable interface to be
 * sorted at the global event queue
 */

public class Event implements Comparable<Object> {
	byte type;
	long count;
	String str;
	double occurTime;
	int sourceId;
	int destId;
	long eventId;

	public Event(byte type, long count, double occurTime, int sourceId,
			String str, int destId, long eventId) {
		this.type = type;
		this.count = count;
		this.occurTime = occurTime;
		this.sourceId = sourceId;
		this.str = str;
		this.destId = destId;
		this.eventId = eventId;
	}

	public int compareTo(Object obj) {
		Event event = (Event) obj;
		if (occurTime > event.occurTime) // sorted based on the occurrence time
		{
			return 1;
		} else if (occurTime < event.occurTime) {
			return -1;
		} else if (eventId > event.eventId) // if time is equal, sorted based on
											// the event id
		{
			return 1;
		} else if (eventId < event.eventId) {
			return -1;
		} else {
			return 0;
		}
	}
}