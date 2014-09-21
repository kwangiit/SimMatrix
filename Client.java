/**
 *
 * @author Ke Wang Since June 2011
 */

/*
 * this class is the Client who can submit tasks
 */
import java.util.*;

public class Client {
	int clientId;
	long numLeftTasks;
	long numTaskRecv;
	boolean waitFlag;
	HashMap<Long, ArrayList<Long>> adjList;
	HashMap<Long, Long> inDegree;
	
	public Client(int clientId, long numLeftTasks, long numTaskRecv) {
		this.clientId = clientId;
		this.numLeftTasks = numLeftTasks;
		this.numTaskRecv = numTaskRecv;
		this.waitFlag = false;
		this.adjList = new HashMap<Long, ArrayList<Long>>();
		this.inDegree = new HashMap<Long, Long>();
	}

	/* client submits tasks to the dispatcher */
	public void submitTaskToDispatcher(double simuTime, int dispatcherId,
			boolean cs) {
		double msgSize = 0;
		double submitLat = 0;
		long numTaskToSubmit = Library.numTaskToSubmit;
		if (numTaskToSubmit > numLeftTasks) {
			numTaskToSubmit = (int) numLeftTasks;
		}
		if (numTaskToSubmit == 0) {
			return;
		}
		msgSize = (double) numTaskToSubmit * (double) Library.taskSize;
		submitLat = msgSize * 8 / Library.linkSpeed + Library.netLat;
		Event submission = new Event((byte) 0, numTaskToSubmit, simuTime
				+ submitLat, clientId, dispatcherId, Library.eventId++);

		/*
		 * if it is the centralized engine, insert submission event to
		 * centralized global queue otherwise, insert submission event to the
		 * distribute global queue
		 */
		if (cs) {
			CentralSimulator.add(submission);
		} else {
			DistributedSimulator.add(submission);
			waitFlag = true;
		}
		Library.numTaskSubmitted += numTaskToSubmit;
		numLeftTasks -= numTaskToSubmit;
	}
	
	
	public void genBotAdjlist() {
		for (long i = 0; i < numLeftTasks; i++)
			adjList.put(i, null);
	}
	
	public void genFanoutAdjlist(int dagPara) {
		long next = - 1;
		for (long i = 0; i < numLeftTasks; i++) {
			ArrayList<Long> newList = new ArrayList<Long>();
			for (long j = 0; j <= dagPara; j++) {
				next = i * dagPara + j;
				if (next >= numLeftTasks)
					break;
				else
					newList.add(next);
			}
			adjList.put(i, newList);
		}
	}
	
	public void genFaninAdjlist(int dagPara) {
		HashMap<Long, ArrayList<Long>> hmTmp = new HashMap<Long, ArrayList<Long>>();
		genFanoutAdjlist(dagPara);
		for (long i = 0; i < numLeftTasks; i++) {
			long reverseId = numLeftTasks - i - 1;
			ArrayList<Long> newList = new ArrayList<Long>();
			newList.add(reverseId);
			ArrayList<Long> tmpList = adjList.get(i);
			for (long j = 0; j < tmpList.size(); j++)
				hmTmp.put(numLeftTasks - 1 - tmpList.get(
						tmpList.size() - 1 - (int)j), newList);
		}
		hmTmp.put(numLeftTasks - 1, null);
		adjList = hmTmp;
	}
	
	public void genPipelineAdjlist(int dagPara) {
		long numPipe = numLeftTasks / dagPara, index = -1, next = -1;
		for (long i = 0; i < numPipe; i++) {
			for (long j = 0; j < dagPara; j++) {
				index = i * dagPara + j;
				next = index + 1;
				ArrayList<Long> newList = new ArrayList<Long>();
				if (next % dagPara != 0 && next < numLeftTasks)
					newList.add(next);
				adjList.put(index, newList);
			}
		}
		
		for (index = numPipe * dagPara; index < numLeftTasks; index++) {
			next = index + 1;
			ArrayList<Long> newList = new ArrayList<Long>();
			if (next % dagPara != 0 && next < numLeftTasks)
				newList.add(next);
			adjList.put(index, newList);
		}
	}
	
	public void genDagAdjlist(String dagType, int dagPara) {
		if (dagType.equals("BOT"))
			genBotAdjlist();
		else if (dagType.equals("FanOut"))
			genFanoutAdjlist(dagPara);
		else if (dagType.equals("FanIn"))
			genFaninAdjlist(dagPara);
		else if (dagType.equals("Pipeline"))
			genPipelineAdjlist(dagPara);
	}
}