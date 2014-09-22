import java.util.Comparator;

public class PQComparator implements Comparator<TaskDesc>{
	public int compare(TaskDesc t1, TaskDesc t2) {
		if (t1.dataLength < t2.dataLength)
			return -1;
		else if (t1.dataLength > t2.dataLength)
			return 1;
		else 
			return 0;
	}
}

class TaskDesc {
	int taskId;
	String users;
	String dir;
	String cmd;
	int dataLength;
}
