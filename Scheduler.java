/**
 *
 * @author Ke Wang Since June 2011
 */

/*
 * this class is the compute node of the distributed simulator
 */

import java.util.*;

public class Scheduler {
	int id;
	int numCore;
	int numIdleCore;

	LinkedList<Integer> waitTaskList;
	PriorityQueue<TaskDesc> localReadyTaskPQ;
	PriorityQueue<TaskDesc> sharedReadyTaskPQ;
	LinkedList<Integer> completeTaskList;

	boolean[] target;
	int[] neighId;
	int numTaskDispatched;
	double pollInterval;
	int numNeighAsked;
	int maxLoad;
	int maxLoadSchedIdx;
	boolean ws;

	HashMap<Object, Object> kvsHM;
	double kvsMaxProcTime, kvsMaxFwdTime;

	HashMap<String, String> dataHM;
	double schedMaxProcTime, schedMaxFwdTime;

	int numTaskFinished;

	public Scheduler(int id, int numCore, int numNeigh) {
		this.id = id;
		this.numCore = numCore;
		this.numIdleCore = numCore;
		this.neighId = new int[numNeigh];
		this.waitTaskList = new LinkedList<Integer>();
		this.localReadyTaskPQ = new PriorityQueue<TaskDesc>(new PQComparator());
		this.sharedReadyTaskPQ = new PriorityQueue<TaskDesc>(new PQComparator());
		this.completeTaskList = new LinkedList<Integer>();
		this.numTaskDispatched = 0;
		this.numTaskFinished = 0;
		this.pollInterval = 0.01;
		ws = false;
		kvsHM = new HashMap<Object, Object>();
		kvsMaxProcTime = kvsMaxFwdTime = 0.0;
		dataHM = new HashMap<String, String>();
		schedMaxProcTime = schedMaxFwdTime = 0.0;
	}

	/* select neighbors to do work stealing */
	public void selectNeigh(boolean[] target) {
		for (int i = 0; i < Library.numNeigh; i++) {
			neighId[i] = (int) (Math.random() * Library.numComputeNode);

			/*
			 * if the chosen node is itself, or has already been chosen, then
			 * choose again
			 */
			while (neighId[i] == id || target[neighId[i]]) {
				neighId[i] = (int) (Math.random() * Library.numComputeNode);
			}
			target[neighId[i]] = true;
		}
	}

	/*
	 * reset the boolean flags to be all false in case to choose neighbors next
	 * time
	 */
	public void resetTarget(boolean[] target) {
		for (int i = 0; i < Library.numNeigh; i++) {
			target[neighId[i]] = false;
		}
	}

	/* execute a task */
	public void executeTask(double length, double simuTime) {
		/* insert a task end event to be processed later */
		Message taskEnd = new Message((byte) 2, -1, simuTime + length, id, id,
				Library.eventId++);
		SimMatrix.add(taskEnd);
		Library.numBusyCore++;
		Library.waitQueueLength--;
	}

	/* execute tasks */
	public void execute(double simuTime) {
		int numTaskToExecute = numIdleCore;
		if (readyTaskListSize < numTaskToExecute) {
			numTaskToExecute = readyTaskListSize;
		}
		double length = 0;
		for (int i = 0; i < numTaskToExecute; i++) {
			/*
			 * generate the task length with uniform random distribution, raning
			 * from [0, Library.maxTaskLength)
			 */
			length = Math.random() * Library.maxTaskLength;
			this.executeTask(length, simuTime);
		}
		numIdleCore -= numTaskToExecute;
		readyTaskListSize -= numTaskToExecute;
	}

	/* poll the neighbors to get the load information to steal tasks */
	public void askLoadInfo(Scheduler[] nodes) {
		int maxLoad = -Library.numCorePerNode;
		int curLoad = 0;
		int maxLoadNodeId = 0;
		double totalLat = 0.0;
		selectNeigh(Library.target);
		resetTarget(Library.target);
		for (int i = 0; i < Library.numNeigh; i++) {
			curLoad = nodes[neighId[i]].readyTaskListSize
					- nodes[neighId[i]].numIdleCore;
			if (curLoad > maxLoad) {
				maxLoad = curLoad;
				maxLoadNodeId = nodes[neighId[i]].id;
			}
		}

		/* if the most heavist loaded neighbor has more available tasks */
		if (maxLoad > 1) {
			// latency due to asking and receiving Load info, and to the request
			// of Jobs
			totalLat = (2 * Library.numNeigh + 1) * Library.stealMsgCommTime;
			Library.numMsg = Library.numMsg + 2 * Library.numNeigh + 1;

			// send a message to requst tasks
			Message requestTask = new Message((byte) 4, -1,
					SimMatrix.getSimuTime() + totalLat, id, maxLoadNodeId,
					Library.eventId++);
			SimMatrix.add(requestTask);
			Library.numWorkStealing++;
			// set the poll interval back to 0.01
			pollInterval = 0.01;
		} else // no neighbors have more available tasks
		{
			totalLat = 2 * Library.numNeigh * Library.stealMsgCommTime;
			Library.numMsg += 2 * Library.numNeigh;

			totalLat += pollInterval; // wait for poll interval time to do work
										// stealing againg
			pollInterval *= 2; // double the poll interval
			Message stealEvent = new Message((byte) 3, -1,
					SimMatrix.getSimuTime() + totalLat, id, -2,
					Library.eventId++);
			SimMatrix.add(stealEvent);
		}
	}

	public void sendMsg(Message msg, double time) {
		if (msg.sourceId == msg.destId)
			msg.occurTime = time;
		else {
			byte[] msgByte = Library.serialize(msg);
			msg.occurTime = time + Library.getCommOverhead(msgByte.length);
		}
		SimMatrix.add(msg);
	}

	public void kvsClientInteract(Pair pair) {
		schedMaxFwdTime = Library.updateTime(Library.packOverhead,
				schedMaxFwdTime);
		int destId = Library.hashServer(pair.key);
		Message msg = new Message("kvs", 0, null, pair, 0, id, destId,
				Library.eventId++);
		sendMsg(msg, schedMaxFwdTime);
	}

	public void checkReadyTask() {
		Integer taskId = waitTaskList.pollFirst();
		if (taskId != null) {
			Pair pair = new Pair(taskId, null, null, taskId, "lookup",
					"metadata:check ready");
			kvsClientInteract(pair);
		}
	}

	public KVSRetObj procKVSEventAct(Pair pair) {
		KVSRetObj kvsRetObj = new KVSRetObj();
		kvsRetObj.key = pair.key;
		kvsRetObj.identifier = pair.identifier;
		kvsRetObj.type = pair.type;
		kvsRetObj.forWhat = pair.forWhat;

		if (pair.type.equals("insert")) {
			kvsHM.put(pair.key, pair.value);
			kvsRetObj.value = pair.value;
			kvsRetObj.result = true;
		} else if (pair.type.equals("lookup")) {
			kvsRetObj.value = kvsHM.get(pair.key);
			kvsRetObj.result = true;
		} else if (pair.type.equals("compare and swap")) {
			TaskMetaData curTaskMD = (TaskMetaData) kvsHM.get(pair.key);
			if (curTaskMD.compareTaskMetaData((TaskMetaData) pair.value) == 0) {
				kvsHM.put(pair.key, pair.attemptValue);
				kvsRetObj.result = true;
			} else {
				kvsRetObj.result = false;
			}
			kvsRetObj.value = curTaskMD;
		}

		return kvsRetObj;
	}

	public void procKVSEvent(Message msg) {
		Pair kvsPair = (Pair) msg.obj;
		KVSRetObj kvsRetObj = procKVSEventAct(kvsPair);
		kvsMaxFwdTime = Library.updateTime(Library.unpackOverhead, kvsMaxFwdTime);
		kvsMaxProcTime = Library.timeCompareOverried(kvsMaxProcTime, kvsMaxFwdTime);
		kvsMaxProcTime = Library.updateTime(Library.procTimePerKVSRequest, kvsMaxProcTime);
		kvsMaxFwdTime = Library.timeCompareOverried(kvsMaxFwdTime, kvsMaxProcTime);
		kvsMaxFwdTime = Library.updateTime(Library.packOverhead, kvsMaxFwdTime);
		Message kvsRetMsg = new Message("kvs return", 0, 
				null, kvsRetObj, 0, id, msg.sourceId, Library.eventId++);
		sendMsg(kvsRetMsg, kvsMaxFwdTime);
	}
	
	public void procKVSRetEvent(Message msg) {
		schedMaxFwdTime = Library.updateTime(Library.unpackOverhead, schedMaxFwdTime);
		KVSRetObj kvsRetObj = (KVSRetObj)msg.obj;
		if (!kvsRetObj.result) {
			if (kvsRetObj.type.equals("compare and swap")) {
				// do something
			} else {
				Pair retryPair = new Pair(kvsRetObj.key, kvsRetObj.value, 
						null, kvsRetObj.identifier, kvsRetObj.type, kvsRetObj.forWhat);
				kvsClientInteract(retryPair);
			}
		} else {
			if (kvsRetObj.type.equals("lookup")) {
				if (kvsRetObj.forWhat.equals("metadata:check ready")) {
					
				}
				//else if {
					
				//}
			}
		}
	}
	
	public void procCheckReadyTaskEvent(Message event) {
		kvsMaxFwdTime = Library.updateTime(Library.unpackOverhead,
				kvsMaxFwdTime);
		kvsMaxProcTime = Library.timeCompareOverried(kvsMaxProcTime,
				kvsMaxFwdTime);
		kvsMaxProcTime = Library.updateTime(Library.procTimePerKVSRequest,
				kvsMaxProcTime);
		Task taskMD = (Task) (kvsHM.get(event.obj));
		Message resCheckReadyTask = new Message((byte) 2, event.info, taskMD,
				0, id, event.sourceId, Library.eventId++);
		kvsMaxFwdTime = Library.timeCompareOverried(kvsMaxFwdTime,
				kvsMaxProcTime);
		kvsMaxFwdTime = Library.updateTime(Library.packOverhead, kvsMaxFwdTime);
		if (event.sourceId == event.destId) {
			resCheckReadyTask.occurTime = kvsMaxFwdTime;
			SimMatrix.add(resCheckReadyTask);
		} else
			sendMsg(resCheckReadyTask, kvsMaxFwdTime);
	}

	public int taskReadyProcess(Task taskMD, TaskDesc td, double time) {
		int flag = 2;
		if (taskMD.allDataSize <= Library.dataSizeThreshold) {
			td.dataLength = taskMD.allDataSize;
			flag = 0;
		} else {
			int maxDataSize = taskMD.dataSize.get(0);
			int maxDataSchedId;
			String key;
			for (int i = 1; i < taskMD.dataSize.size(); i++) {
				if (taskMD.dataSize.get(i) > maxDataSize) {
					maxDataSize = taskMD.dataSize.get(i);
					maxDataSchedId = taskMD.parent.get(i);
					key = taskMD.dataNameList.get(i);
				}
			}
			td.dataLength = maxDataSize;
			if (maxDataSchedId == id)
				flag = 1;
			else {
				boolean taskPush = true;
				if (dataHM.containsKey(key)) {
					flag = 1;
					taskPush = false;
				} else
					taskPush = true;
				if (taskPush) {
					schedMaxFwdTime = Library.updateTime(
							time - SimMatrix.getSimuTime()
									+ Library.packOverhead, schedMaxFwdTime);
					Message pushTaskEvent = new Message((byte) 4, td.taskId,
							td, 0, id, maxDataSchedId, Library.eventId++);
					sendMsg(pushTaskEvent, schedMaxFwdTime);
					flag = 2;
				}
			}
		}
		return flag;
	}

	public void executeTask(TaskDesc td) {

	}

	public void procResCheckReadyTaskEvent(KVSRetObj kvsRetObj) {
		//double time = SimMatrix.getSimuTime() + Library.unpackOverhead + Library.procTimePerTask;
		TaskMetaData taskMD = (TaskMetaData) (kvsRetObj.value);
		if (taskMD.indegree > 0)
			waitTaskList.add((Integer)(kvsRetObj.identifier));
		else {
			TaskDesc td = new TaskDesc();
			td.taskId = taskMD.taskId;
			int flag = taskReadyProcess(taskMD, td, time);
			if (flag == 0 || flag == 1) {
				if (numIdleCore > 0) {
					numIdleCore--;
					/*
					 * Event TaskDoneEvent = new Event((byte)5, td.taskId, td,
					 * time + Math.random() * Library.maxTaskLength, nodeId,
					 * nodeId, Library.eventId++);
					 * DistributedSimulator.add(TaskDoneEvent);
					 */
				} else if (flag == 0)
					localReadyTaskPQ.add(td);
				else
					sharedReadyTaskPQ.add(td);
			}
		}
		schedMaxFwdTime = Library.timeCompareOverried(schedMaxFwdTime, time);
		checkReadyTask();
	}
}