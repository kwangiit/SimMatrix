/**
 *
 * @author Ke Wang Since June 2011
 */

/*
 * this class is the Client who can submit tasks
 */
import java.util.*;
import java.util.Map.Entry;

public class Client {
	int clientId;
	int numLeftTasks;
	//long numTaskRecv;
	boolean waitFlag;
	HashMap<Integer, ArrayList<Integer>> adjList;
	HashMap<Integer, Integer> inDegree;
	
	public Client(int clientId, int numLeftTasks){//, long numTaskRecv) {
		this.clientId = clientId;
		this.numLeftTasks = numLeftTasks;
		//this.numTaskRecv = numTaskRecv;
		this.waitFlag = false;
		this.adjList = new HashMap<Integer, ArrayList<Integer>>();
		this.inDegree = new HashMap<Integer, Integer>();
	}

	/* client submits tasks to the dispatcher */
	public void submitTaskToDispatcher(double simuTime, int dispatcherId, boolean cs) {
		double msgSize = 0;
		double submitLat = 0;
		long numTaskToSubmit = Library.numTaskToSubmit;
		if (numTaskToSubmit > numLeftTasks) {
			numTaskToSubmit = (int) numLeftTasks;
		}
		if (numTaskToSubmit == 0) {
			return;
		}
		msgSize = (double) numTaskToSubmit * (double) Library.oneMsgSize;
		submitLat = msgSize * 8 / Library.linkSpeed + Library.netLat;
		Event submission = new Event((byte) 0, numTaskToSubmit, null, simuTime
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
		for (int i = 0; i < numLeftTasks; i++)
			adjList.put(i, null);
	}
	
	public void genFanoutAdjlist(int dagPara) {
		int next = - 1;
		for (int i = 0; i < numLeftTasks; i++) {
			ArrayList<Integer> newList = new ArrayList<Integer>();
			for (int j = 0; j <= dagPara; j++) {
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
		HashMap<Integer, ArrayList<Integer>> hmTmp = new HashMap<Integer, ArrayList<Integer>>();
		genFanoutAdjlist(dagPara);
		for (int i = 0; i < numLeftTasks; i++) {
			int reverseId = numLeftTasks - i - 1;
			ArrayList<Integer> newList = new ArrayList<Integer>();
			newList.add(reverseId);
			ArrayList<Integer> tmpList = adjList.get(i);
			for (int j = 0; j < tmpList.size(); j++)
				hmTmp.put(numLeftTasks - 1 - tmpList.get(
						tmpList.size() - 1 - j), newList);
		}
		hmTmp.put(numLeftTasks - 1, null);
		adjList = hmTmp;
	}
	
	public void genPipelineAdjlist(int dagPara) {
		int numPipe = numLeftTasks / dagPara, index = -1, next = -1;
		for (int i = 0; i < numPipe; i++) {
			for (int j = 0; j < dagPara; j++) {
				index = i * dagPara + j;
				next = index + 1;
				ArrayList<Integer> newList = new ArrayList<Integer>();
				if (next % dagPara != 0 && next < numLeftTasks)
					newList.add(next);
				adjList.put(index, newList);
			}
		}
		
		for (index = numPipe * dagPara; index < numLeftTasks; index++) {
			next = index + 1;
			ArrayList<Integer> newList = new ArrayList<Integer>();
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
	
	public void genDagIndegree() {
		Iterator<Entry<Integer, ArrayList<Integer>>> it = adjList.entrySet().iterator();
		for (int i = 0; i < adjList.size(); i++)
			inDegree.put(i, 0);
		while (it.hasNext()) {
			Entry<Integer, ArrayList<Integer>> entry = it.next();
			int index = entry.getKey();
			ArrayList<Integer> existList = entry.getValue();
			for (int j = 0; j < existList.size(); j++)
				inDegree.put(existList.get(j), inDegree.get(
						existList.get(j)).intValue() + 1);
		}
	}
	
	public void insertTaskMetaToKVS(Scheduler[] cn) {
		Iterator<Entry<Integer, ArrayList<Integer>>> it = adjList.entrySet().iterator();
		while (it.hasNext()) {
			Entry<Integer, ArrayList<Integer>> entry = it.next();
			TaskMetaData task = new TaskMetaData();
			task.taskId = entry.getKey();
			task.indegree = inDegree.get(task.taskId);
			task.children = entry.getValue();
			int kvsServerId = Library.hashServer(task.taskId);
			cn[kvsServerId].kvsHM.put(task.taskId, task);
		}
	}
	
	public void splitTask(Scheduler[] cn) {
		for (int i = 0; i < Library.numAllTask; i++) {
			int idx = i % Library.numComputeNode;
			cn[idx].waitTaskList.add(idx);
		}
	}
}