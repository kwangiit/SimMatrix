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
	boolean completeQueueBusy;

	HashMap<Object, Object> kvsHM;
	double kvsMaxProcTime, kvsMaxFwdTime;

	HashMap<String, String> dataHM;
	double localTime;
	//double schedMaxProcTime, schedMaxFwdTime;

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
		localTime = 0.0;
		//schedMaxProcTime = schedMaxFwdTime = 0.0;
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
		//schedMaxFwdTime = Library.updateTime(Library.packOverhead,
				//schedMaxFwdTime);
		int destId = Library.hashServer(pair.key);
		Message msg = new Message("kvs", 0, null, pair, 0, id, destId,
				Library.eventId++);
		//sendMsg(msg, schedMaxFwdTime);
		sendMsg(msg, localTime + Library.packOverhead);
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
		//schedMaxFwdTime = Library.updateTime(Library.unpackOverhead, schedMaxFwdTime);
		localTime = SimMatrix.getSimuTime() + Library.unpackOverhead;
		KVSRetObj kvsRetObj = (KVSRetObj)msg.obj;
		if (!kvsRetObj.result) {
			if (kvsRetObj.type.equals("compare and swap")) {
				if (kvsRetObj.forWhat.equals("metadata:children task")) {
					procChildMDRetEvent(kvsRetObj);
				}
			} else {
				Pair retryPair = new Pair(kvsRetObj.key, kvsRetObj.value, 
						null, kvsRetObj.identifier, kvsRetObj.type, kvsRetObj.forWhat);
				kvsClientInteract(retryPair);
			}
		} else {
			if (kvsRetObj.type.equals("lookup")) {
				if (kvsRetObj.forWhat.equals("metadata:check ready")) {
					procCheckReadyRetEvent(kvsRetObj);
				} else if (kvsRetObj.forWhat.equals("metadata:execute task")) {
					procExecuteTaskRetEvent(kvsRetObj);
				} else if (kvsRetObj.forWhat.equals("metadata:notify children")){
					procNotifyChildRetEvent(kvsRetObj);
				} else if (kvsRetObj.forWhat.equals("metadata:children task")) {
					procChildMDRetEvent(kvsRetObj);
				}
			} else if (kvsRetObj.type.equals("compare and swap")) {
				if (kvsRetObj.forWhat.equals("metadata:children task")) {
					updateChildMDSuc(kvsRetObj);
				}
			}
		}
	}
	
	public int mapReadyTask(TaskMetaData taskMD, TaskDesc td) {
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
					//schedMaxFwdTime = Library.updateTime(Library.packOverhead, schedMaxFwdTime);
					Message pushTaskMsg = new Message("push task", td.taskId, 
							null, td, 0, id, maxDataSchedId, Library.eventId++);
					sendMsg(pushTaskMsg, localTime + Library.packOverhead);
					flag = 2;
				}
			}
		}
		return flag;
	}

	public void executeTask(TaskDesc td) {
		int taskId = td.taskId;
		Pair pair = new Pair(taskId, null, null, td, "lookup", "metadata:execute task");
		kvsClientInteract(pair);
	}
	
	public void tryExecAnotherTask() {
		TaskDesc td = null;
		if (localReadyTaskPQ.size() > 0) {
			td = localReadyTaskPQ.poll();
		} else if (sharedReadyTaskPQ.size() > 0) {
			td = sharedReadyTaskPQ.poll();
		} else if (!ws){
			SimMatrix.add(new Message("work steal", -1, null, 
					null, localTime, id, -2, Library.eventId++));
		}
		if (td != null) {
			executeTask(td);
		}
	}
	
	public void updateChildMDSuc(KVSRetObj kvsRetObj) {
		Task task = Library.globalTaskHM.get(kvsRetObj.identifier);
		task.numChildMDUpdated++;
		if (task.numChildMDUpdated < task.taskMD.children.size()) {
			updateChildMetadata(task.taskMD.children.get(task.numChildMDUpdated), task.taskId);
		} else if (completeTaskList.size() > 0) {
			int taskId = completeTaskList.pollFirst();
			notifyChildren(taskId);
		} else
			completeQueueBusy = true;
	}
	
	public void procChildMDRetEvent(KVSRetObj kvsRetObj) {
		TaskMetaData childTaskMD = (TaskMetaData)kvsRetObj.value;
		TaskMetaData childTaskMDAttempt = childTaskMD.copyTaskMetaData();
		childTaskMDAttempt.indegree--;
		childTaskMDAttempt.parent.add(id);
		childTaskMDAttempt.dataNameList.add(kvsRetObj.identifier.toString());
		childTaskMDAttempt.dataSize.add(1000000);
		childTaskMDAttempt.allDataSize += 1000000;
		Pair pair = new Pair(childTaskMD.taskId, childTaskMD, childTaskMDAttempt, 
				kvsRetObj.identifier, "compare and swap", kvsRetObj.forWhat);
		kvsClientInteract(pair);
	}
	
	public void updateChildMetadata(int childTaskID, int taskID) {
		Pair pair = new Pair(childTaskID, null, null, taskID, "lookup", "metadata:children task");
		kvsClientInteract(pair);
	}
	
	public void procNotifyChildRetEvent(KVSRetObj kvsRetObj) {
		TaskMetaData tm = (TaskMetaData)kvsRetObj.value;
		if (tm.children.size() > 0) {
			updateChildMetadata(tm.children.get(0), tm.taskId);
		} else if (completeTaskList.size() > 0) {
				int taskId = completeTaskList.pollFirst();
				notifyChildren(taskId);
		} else
			completeQueueBusy = true;
	}
	
	public void notifyChildren(int taskId) {
		Pair pair = new Pair(taskId, null, null, taskId, "lookup", "metadata:notify children");
		kvsClientInteract(pair);
	}
	
	public void actExecuteTask(TaskDesc td) {
		Library.numTaskFinished++;
		localTime += Math.random() * Library.maxTaskLength;
		numIdleCore++;
		tryExecAnotherTask();
		if (!completeQueueBusy){
			completeQueueBusy = true;
			notifyChildren(td.taskId);
		} else
			completeTaskList.add(td.taskId);
	}
	
	public String requestData(int taskId, int parentId, 
			TaskDesc td, int dataSize, String dataName) {
		String data = null;
		if (dataSize > 0) {
			if (id == parentId || dataHM.containsKey(dataName))
				data = dataHM.get(dataName);
			else {
				Message reqDataMsg = new Message("request data", taskId, 
						dataName, td, 0, id, parentId, Library.eventId++); 
				sendMsg(reqDataMsg, localTime + Library.packOverhead);
			}
		}
		return data;
	}
	
	public void get
	
	public void procExecuteTaskRetEvent(KVSRetObj kvsRetObj) {
		TaskMetaData taskMD = (TaskMetaData)kvsRetObj.value;
		Task task = new Task();
		task.taskId = taskMD.taskId;
		task.taskMD = taskMD;
		task.numParentDataRecv = 0;
		task.data = new String();
		task.numChildMDUpdated = 0;
		//Library.globalTaskHM.put(task.taskId, task);
		int i = 0;
		TaskDesc td = (TaskDesc)(kvsRetObj.identifier);
		String data = requestData(task.taskId, taskMD.parent.get(i), td,
				taskMD.dataSize.get(i), taskMD.dataNameList.get(i));
		while (data != null && i < taskMD.parent.size() || taskMD.dataSize.get(i) == 0) {
			if (taskMD.dataSize.get(i) > 0)
				localTime += Library.procTimePerKVSRequest;
			task.numParentDataRecv++;
			i++;
			task.data += data;
			data = requestData(task.taskId, taskMD.parent.get(i), td,
					taskMD.dataSize.get(i), taskMD.dataNameList.get(i));
		}
		if (task.numParentDataRecv == taskMD.parent.size()) {
			actExecuteTask(td);
		}
		
		Library.globalTaskHM.put(task.taskId, task);
	}
	
	public void procCheckReadyRetEvent(KVSRetObj kvsRetObj) {
		TaskMetaData taskMD = (TaskMetaData) (kvsRetObj.value);
		localTime += Library.procTimePerTask;
		if (taskMD.indegree > 0)
			waitTaskList.add((Integer)(kvsRetObj.identifier));
		else {
			TaskDesc td = new TaskDesc();
			td.taskId = taskMD.taskId;
			int flag = mapReadyTask(taskMD, td);
			if (flag == 0 || flag == 1) {
				if (numIdleCore > 0) {
					numIdleCore--;
					executeTask(td);
				} else if (flag == 0)
					localReadyTaskPQ.add(td);
				else
					sharedReadyTaskPQ.add(td);
			}
		}
		checkReadyTask();
	}
	
	public void procRetReqDataEvent(Message msg) {
		localTime = SimMatrix.getSimuTime() + Library.unpackOverhead;
		
	}
	
	public void procReqDataEvent(Message msg) {
		localTime = SimMatrix.getSimuTime() + Library.unpackOverhead;
		localTime += Library.procTimePerKVSRequest;
		String data = dataHM.get(msg.content);
		Message msg = new Message("return req data", msg.info, 
				data, msg.obj, 0, id, msg.sourceId, Library.eventId++);
		sendMsg(msg, localTime + Library.packOverhead + Library.getCommOverhead(1000000));
	}
}