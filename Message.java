/**
 *
 * @author Ke Wang Since June 2011
 */

/*
 * this is the class of event, implementing the comparable interface to be
 * sorted at the global event queue
 */
import java.util.*;
import java.io.Serializable;

public class Message implements Comparable<Object> {
	String type;
	int info;
	String content;
	Object obj;
	double occurTime;
	int sourceId;
	int destId;
	long eventId;
	
	public Message(String type, int info, String content, Object obj, 
			double occurTime, int sourceId, int destId, long eventId) {
		this.type = type;
		this.info = info;
		this.content = content;
		this.obj = obj;
		this.occurTime = occurTime;
		this.sourceId = sourceId;
		this.destId = destId;
		this.eventId = eventId;
	}

	public int compareTo(Object obj) {
		Message event = (Message) obj;
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

class Pair {
	Object key;
	Object value;
	Object attemptValue;
	Object identifier;
	String type;
	String forWhat;
	
	Pair () {
	}
	
	Pair(Object key, Object value, Object attemptValue, 
			Object identifier, String type, String forWhat) {
		this.key = key;
		this.value = value;
		this.attemptValue = attemptValue;
		this.identifier = identifier;
		this.type = type;
		this.forWhat = forWhat;
	}
}

@SuppressWarnings("serial")
class KVSRetObj implements Serializable {
	Object key;
	Object value;
	Object identifier;
	String type;
	String forWhat;
	boolean result;
}