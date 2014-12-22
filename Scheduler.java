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

	//boolean[] target;
	int[] neighId;
	double pollInterval;
	int maxLoad;
	int maxLoadSchedIdx;
	boolean ws;
	
	boolean completeTaskBusy;

	HashMap<Object, Object> kvsHM;
	double kvsMaxProcTime, kvsMaxFwdTime;

	HashMap<String, String> dataHM;
	double schedMaxProcTime, schedMaxFwdTime;
	double localTime;

	int numTaskFinished;
	int numTaskTransmit;
	double soFarThroughput;
	double localQueueTimeThreshold;

	public Scheduler(int id) {
		this.id = id;
		numCore = Library.numCorePerNode;
		numIdleCore = numCore;
		waitTaskList = new LinkedList<Integer>();
		localReadyTaskPQ = new PriorityQueue<TaskDesc>(new PQComparator());
		sharedReadyTaskPQ = new PriorityQueue<TaskDesc>(new PQComparator());
		completeTaskList = new LinkedList<Integer>();
		//target = new boolean[Library.numComputeNode];
		neighId = new int[Library.numNeigh];
		pollInterval = Library.initPollInterval;
		ws = false;
		completeTaskBusy = false;
		kvsHM = new HashMap<Object, Object>();
		kvsMaxProcTime = kvsMaxFwdTime = 0.0;
		dataHM = new HashMap<String, String>();
		schedMaxProcTime = schedMaxFwdTime = 0.0;
		localTime = 0.0;
		numTaskTransmit = 0;
		numTaskFinished = 0;
		soFarThroughput = 0.0;
		localQueueTimeThreshold = Library.localQueueTimeThreshold;
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
		int destId = Library.hashServer(pair.key);
		Message msg = new Message("kvs", 0, null, pair, 0, id, destId, Library.eventId++);
		localTime += Library.packOverhead;
		sendMsg(msg, localTime);
	}

	public void checkReadyTask() {
		Integer taskId = waitTaskList.pollFirst();
		if (taskId != null) {
			Pair pair = new Pair(taskId, null, null, taskId, "lookup", "metadata:check ready");
			kvsClientInteract(pair);
		}
	}
	
	public int mapReadyTask(TaskMetaData taskMD, TaskDesc td) {
		int flag = 2;
		if (taskMD.allDataSize * 8 / td.taskLength <= Library.ratioThreshold) {
			td.dataLength = taskMD.allDataSize;
			flag = 1;
		} else {
			int maxDataSize = taskMD.dataSize.get(0);
			int maxDataSchedId = taskMD.parent.get(0);
			String key = taskMD.dataNameList.get(0);
			for (int i = 1; i < taskMD.dataSize.size(); i++) {
				if (taskMD.dataSize.get(i) > maxDataSize) {
					maxDataSize = taskMD.dataSize.get(i);
					maxDataSchedId = taskMD.parent.get(i);
					key = taskMD.dataNameList.get(i);
				}
			}
			td.dataLength = maxDataSize;
			if (maxDataSize * 8 / td.taskLength <= Library.ratioThreshold)
				flag = 1;
			else if (maxDataSchedId == id)
				flag = 0;
			else {
				boolean taskPush = true;
				if (dataHM.containsKey(key)) {
					flag = 0;
					taskPush = false;
				} else
					taskPush = true;
				if (taskPush) {
					Message pushTaskMsg = new Message("push task", td.taskId, 
							null, td, 0, id, maxDataSchedId, Library.eventId++);
					localTime += Library.packOverhead;
					sendMsg(pushTaskMsg, localTime);
					flag = 2;
				}
			}
		}
		return flag;
	}
	
	public void executeTask(TaskDesc td) {
		Task task = Library.globalTaskHM.get(td.taskId);
		task.startExecTime = localTime;
		Pair pair = new Pair(td.taskId, null, null, td, "lookup", "metadata:execute task");
		kvsClientInteract(pair);
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
				localTime += Library.packOverhead;
				sendMsg(reqDataMsg, localTime);
			}
		}
		return data;
	}
	
	public void tryExecAnotherTask() {
		TaskDesc td = null;
		if (localReadyTaskPQ.size() > 0) {
			td = localReadyTaskPQ.poll();
		} else if (sharedReadyTaskPQ.size() > 0) {
			td = sharedReadyTaskPQ.poll();
		} else if (!ws && Library.numComputeNode > 1){
			ws = true;
			SimMatrix.add(new Message("work steal", -1, null, 
					null, localTime, id, id, Library.eventId++));
		}
		if (td != null) {
			Library.readyQueueLength--;
			Library.numBusyCore++;
			numIdleCore--;
			executeTask(td);
		}
	}
	
	public void notifyChildren(int taskId) {
		Pair pair = new Pair(taskId, null, null, taskId, "lookup", "metadata:notify children");
		kvsClientInteract(pair);
	}
	
	public void updateChildMetadata(int childTaskId, int taskId) {
		Pair pair = new Pair(childTaskId, null, null, taskId, "lookup", "metadata:children task");
		kvsClientInteract(pair);
	}
	
	public void procRetNotifyChildEvent(KVSRetObj kvsRetObj) {
		TaskMetaData tm = (TaskMetaData)kvsRetObj.value;
		if (tm.children != null && tm.children.size() > 0) {
			updateChildMetadata(tm.children.get(0), tm.taskId);
		}
		else  {
			completeTaskBusy = false;
			if (completeTaskList.size() > 0) {
				completeTaskBusy = true;
				int taskId = completeTaskList.pollFirst();
				notifyChildren(taskId);
			}
		}
	}
	
	public void procRetChildMDEvent(KVSRetObj kvsRetObj) {
		TaskMetaData childTaskMD = (TaskMetaData)kvsRetObj.value;
		TaskMetaData childTaskMDAttempt = childTaskMD.copyTaskMetaData();
		childTaskMDAttempt.indegree--;
		if (childTaskMDAttempt.parent == null) {
			childTaskMDAttempt.parent = new ArrayList<Integer>();
			childTaskMDAttempt.dataNameList = new ArrayList<String>();
			childTaskMDAttempt.dataSize = new ArrayList<Integer>();
		}
		childTaskMDAttempt.parent.add(id);
		childTaskMDAttempt.dataNameList.add(kvsRetObj.identifier.toString());
		Task task = Library.globalTaskHM.get(kvsRetObj.identifier);
		childTaskMDAttempt.dataSize.add(task.dataOutPutSize);
		childTaskMDAttempt.allDataSize += task.dataOutPutSize;
		Pair pair = new Pair(childTaskMD.taskId, childTaskMD, childTaskMDAttempt, 
				kvsRetObj.identifier, "compare and swap", kvsRetObj.forWhat);
		kvsClientInteract(pair);
	}
	
	public void updateChildMDSuc(KVSRetObj kvsRetObj) {
		Task task = Library.globalTaskHM.get(kvsRetObj.identifier);
		task.numChildMDUpdated++;
		if (task.numChildMDUpdated < task.taskMD.children.size())
			updateChildMetadata(task.taskMD.children.get(task.numChildMDUpdated), task.taskId);
		else {
			completeTaskBusy = false;
			//System.out.println("Now, process the next one!, the size is:" + completeTaskList.size());
			if (completeTaskList.size() > 0) {
				completeTaskBusy = true;
				int taskId = completeTaskList.pollFirst();
				notifyChildren(taskId);
			} 
		}
	}
	
	public void actExecuteTask(TaskDesc td) {
		localTime += td.taskLength;
		Message msg = new Message("task done", td.taskId, 
				null, td, localTime, id, id, Library.eventId);
		SimMatrix.add(msg);
	}
	
	public void procTaskDoneEvent(Message msg) {
		localTime = SimMatrix.getSimuTime();
		Library.numTaskFinished++;
		Library.numBusyCore--;
		numIdleCore++;
		numTaskFinished++;
		dataHM.put(Integer.toString(msg.info), "This is the data!");
		Task task = Library.globalTaskHM.get(msg.info);
		task.endTime = localTime;
		tryExecAnotherTask();
		if (!completeTaskBusy){
			completeTaskBusy = true;
			notifyChildren(msg.info);
		} else {
			completeTaskList.add(msg.info);
		}
	}
	
	public void getTaskData(int pos, TaskDesc td, Task task) {
		if (task.taskMD.parent != null && task.numParentDataRecv < task.taskMD.parent.size()) {
			String data = requestData(task.taskId, task.taskMD.parent.get(pos), td,
				task.taskMD.dataSize.get(pos), task.taskMD.dataNameList.get(pos));
			while (data != null && pos < task.taskMD.parent.size() 
					|| task.taskMD.dataSize.get(pos) == 0) {
				if (task.taskMD.dataSize.get(pos) > 0)
					localTime += Library.procTimePerKVSRequest;
				task.numParentDataRecv++;
				pos++;
				task.data += data;
				if (pos == task.taskMD.parent.size())
					break;
				data = requestData(task.taskId, task.taskMD.parent.get(pos), td,
						task.taskMD.dataSize.get(pos), task.taskMD.dataNameList.get(pos));
			}
		}
		if (task.taskMD.parent == null || task.numParentDataRecv == task.taskMD.parent.size())
			actExecuteTask(td);
		
		Library.globalTaskHM.put(task.taskId, task);
	}
	
	public void procRetReqDataEvent(Message msg) {
		localTime = SimMatrix.getSimuTime() + Library.unpackOverhead;
		Task task = Library.globalTaskHM.get(msg.info);
		task.numParentDataRecv++;
		task.data += msg.content;
		getTaskData(task.numParentDataRecv, (TaskDesc)msg.obj, task);
		dataHM.put(task.taskMD.dataNameList.get(task.numParentDataRecv - 1), msg.content);
	}
	
	public void procReqDataEvent(Message msg) {
		schedMaxFwdTime = Library.updateTime(Library.unpackOverhead, schedMaxFwdTime);
		schedMaxProcTime = Library.timeCompareOverried(schedMaxProcTime, schedMaxFwdTime);
		schedMaxProcTime = Library.updateTime(Library.procTimePerKVSRequest, schedMaxProcTime);
		schedMaxFwdTime = Library.timeCompareOverried(schedMaxFwdTime, schedMaxProcTime);
		schedMaxFwdTime = Library.updateTime(Library.packOverhead, schedMaxFwdTime);
		String data = dataHM.get(msg.content);
		Message retMsg = new Message("return req data", msg.info, 
				data, msg.obj, 0, id, msg.sourceId, Library.eventId++);
		Task task = Library.globalTaskHM.get(Integer.parseInt(msg.content));
		sendMsg(retMsg, schedMaxFwdTime + Library.getCommOverhead(task.dataOutPutSize));
	}
	
	public void procRetExecuteTaskEvent(KVSRetObj kvsRetObj) {
		TaskMetaData taskMD = (TaskMetaData)kvsRetObj.value;
		Task task = Library.globalTaskHM.get(taskMD.taskId);
		task.taskMD = taskMD;
		if (task.data == null)
			task.data = new String();
		int i = 0;
		TaskDesc td = (TaskDesc)(kvsRetObj.identifier);
		getTaskData(i, td, task);
	}
	
	public void monitorLocalQueue() {
		soFarThroughput = (double)numTaskFinished / SimMatrix.getSimuTime();
		double expectTime = (double)localReadyTaskPQ.size() / soFarThroughput;
		TaskDesc[] td = new TaskDesc[localReadyTaskPQ.size()];
		int i = 0;
		while (localReadyTaskPQ.size() > 0)
			td[i++] = localReadyTaskPQ.poll();
		i = 0;
		while (expectTime > Library.localQueueTimeThreshold) {
			sharedReadyTaskPQ.add(td[td.length - 1 - i]);
			i++;
			expectTime = ((double)td.length - i) / soFarThroughput;
		}
		int left = td.length - i;
		for (int j = 0; j < left; j++) 
			localReadyTaskPQ.add(td[j]);
	}
	
	public void procRetCheckReadyEvent(KVSRetObj kvsRetObj) {
		TaskMetaData taskMD = (TaskMetaData) (kvsRetObj.value);
		localTime += Library.procTimePerTask;
		if (taskMD.indegree > 0) {
			waitTaskList.add((Integer)(kvsRetObj.identifier));
			checkReadyTask();
		}
		else {
			TaskDesc td = new TaskDesc();
			td.taskId = taskMD.taskId;
			td.dataLength = 0;
			td.taskLength = Math.random() * Library.maxTaskLength;
			Task task = new Task();
			task.taskId = taskMD.taskId;
			task.taskMD = taskMD;
			task.numParentDataRecv = 0;
			task.numChildMDUpdated = 0;
			task.dataOutPutSize = (int)(Math.random() * (double)10000000);
			task.submissionTime = 0.0;
			task.readyTime = localTime;
			task.startExecTime = 0.0;
			task.endTime = 0.0;
			Library.globalTaskHM.put(task.taskId, task);
			
			int flag = mapReadyTask(taskMD, td);
			if (flag == 0 || flag == 1) {
				if (numIdleCore > 0) {
					Library.numBusyCore++;
					numIdleCore--;
					executeTask(td);
				} else { 
					Library.readyQueueLength++;
					if (flag == 0) {
						localReadyTaskPQ.add(td);
						monitorLocalQueue();
					} else
						sharedReadyTaskPQ.add(td);
				}
				checkReadyTask();
			}
		}
	}
	
	public void procPushTaskEvent(Message msg) {
		schedMaxFwdTime = Library.updateTime(Library.unpackOverhead, schedMaxFwdTime);
		schedMaxFwdTime = Library.updateTime(Library.packOverhead, schedMaxFwdTime);
		Message retMsg = new Message("return push task", msg.info,
				null, null, schedMaxFwdTime, id, msg.sourceId, Library.eventId++);
		SimMatrix.add(retMsg);
		localTime = schedMaxFwdTime;
		TaskDesc td = (TaskDesc)msg.obj;
		if (numIdleCore > 0) {
			numIdleCore--;
			Library.numBusyCore++;
			executeTask(td);
		} else {
			localReadyTaskPQ.add(td);
			Library.readyQueueLength++;
		}
	}
	
	public void procRetPushTaskEvent(Message msg) {
		localTime = SimMatrix.getSimuTime() + Library.unpackOverhead;
		checkReadyTask();
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
		localTime = SimMatrix.getSimuTime() + Library.unpackOverhead;
		KVSRetObj kvsRetObj = (KVSRetObj)msg.obj;
		if (!kvsRetObj.result) {
			if (kvsRetObj.type.equals("compare and swap")) {
				if (kvsRetObj.forWhat.equals("metadata:children task")) {
					procRetChildMDEvent(kvsRetObj);
				}
			} else {
				Pair retryPair = new Pair(kvsRetObj.key, kvsRetObj.value, 
						null, kvsRetObj.identifier, kvsRetObj.type, kvsRetObj.forWhat);
				kvsClientInteract(retryPair);
			}
		} else {
			if (kvsRetObj.type.equals("lookup")) {
				if (kvsRetObj.forWhat.equals("metadata:check ready")) {
					procRetCheckReadyEvent(kvsRetObj);
				} else if (kvsRetObj.forWhat.equals("metadata:execute task")) {
					procRetExecuteTaskEvent(kvsRetObj);
				} else if (kvsRetObj.forWhat.equals("metadata:notify children")){
					TaskMetaData taskMD = (TaskMetaData)kvsRetObj.value;
					procRetNotifyChildEvent(kvsRetObj);
				} else if (kvsRetObj.forWhat.equals("metadata:children task")) {
					procRetChildMDEvent(kvsRetObj);
				}
			} else if (kvsRetObj.type.equals("compare and swap")) {
				if (kvsRetObj.forWhat.equals("metadata:children task")) {
					updateChildMDSuc(kvsRetObj);
				}
			}
		}
	}
		
	public void selectNeigh() {
		for (int i = 0; i < Library.numNeigh; i++) {
			neighId[i] = (int) (Math.random() * Library.numComputeNode);

			/*
			 * if the chosen node is itself, or has already been chosen, then
			 * choose again
			 */
			while (neighId[i] == id || Library.target[neighId[i]]) {
				neighId[i] = (int) (Math.random() * Library.numComputeNode);
			}
			Library.target[neighId[i]] = true;
		}
	}

	/*
	 * reset the boolean flags to be all false in case to choose neighbors next
	 * time
	 */
	public void resetTarget() {
		for (int i = 0; i < Library.numNeigh; i++) {
			Library.target[neighId[i]] = false;
		}
	}

	/* execute a task */
//	public void executeTask(double length, double simuTime) {
//		/* insert a task end event to be processed later */
//		Message taskEnd = new Message((byte) 2, -1, simuTime + length, id, id,
//				Library.eventId++);
//		SimMatrix.add(taskEnd);
//		Library.numBusyCore++;
//		Library.waitQueueLength--;
//	}

	/* execute tasks */
//	public void execute(double simuTime) {
//		int numTaskToExecute = numIdleCore;
//		if (readyTaskListSize < numTaskToExecute) {
//			numTaskToExecute = readyTaskListSize;
//		}
//		double length = 0;
//		for (int i = 0; i < numTaskToExecute; i++) {
//			/*
//			 * generate the task length with uniform random distribution, raning
//			 * from [0, Library.maxTaskLength)
//			 */
//			length = Math.random() * Library.maxTaskLength;
//			this.executeTask(length, simuTime);
//		}
//		numIdleCore -= numTaskToExecute;
//		readyTaskListSize -= numTaskToExecute;
//	}

	/* poll the neighbors to get the load information to steal tasks */
	public void askLoadInfo(Scheduler[] schedulers) {
		int maxLoad = -Integer.MAX_VALUE;
		int curLoad = 0;
		int maxLoadSchedulerId = 0;
		double totalLat = 0.0;
		selectNeigh();
		resetTarget();
		for (int i = 0; i < Library.numNeigh; i++) {
			curLoad = schedulers[neighId[i]].sharedReadyTaskPQ.size();
			if (curLoad > maxLoad) {
				maxLoad = curLoad;
				maxLoadSchedulerId = schedulers[neighId[i]].id;
			}
		}

		/* if the most heaviest loaded neighbor has more available tasks */
		if (maxLoad > 1) {
			// latency due to asking and receiving Load info, and to the request
			// of Jobs
			totalLat = (2 * Library.numNeigh + 1) * (Library.singleMsgTransTime + 
					Library.packOverhead + Library.unpackOverhead);
			Library.numMsg = Library.numMsg + 2 * Library.numNeigh + 1;

			// send a message to requst tasks
			Message reqTaskMsg = new Message("request task", -1, null, null,
					localTime + totalLat, id, maxLoadSchedulerId, Library.eventId++);
			SimMatrix.add(reqTaskMsg);
			Library.numWorkStealing++;
			// set the poll interval back to 0.01
			//pollInterval = 0.01;
		} else { // no neighbors have more available tasks
			totalLat = 2 * Library.numNeigh * (Library.singleMsgTransTime + 
					Library.packOverhead + Library.unpackOverhead);
			Library.numMsg += 2 * Library.numNeigh;

			totalLat += pollInterval; // wait for poll interval time to do work
										// stealing againg
			pollInterval *= 2; // double the poll interval
			if (pollInterval < Library.pollIntervalUB)
			{
				if (!ws && Library.numComputeNode > 1)
				{
					ws = true;
					Message workstealMsg = new Message("work steal", -1, null, null,
							localTime + totalLat, id, id, Library.eventId++);
					SimMatrix.add(workstealMsg);
				}
			}
		}
	}
	
	public void procWorkStealEvent(Message msg, Scheduler[] schedulers) {
		localTime = SimMatrix.getSimuTime();
		if (localReadyTaskPQ.size() + sharedReadyTaskPQ.size() == 0) {
			selectNeigh();
			resetTarget();
			askLoadInfo(schedulers);
		}
	}
	
	public void procRetReqTaskEvent(Message msg) {
		localTime = SimMatrix.getSimuTime() + Library.unpackOverhead;
		if (msg.info > 0) {
			pollInterval = 0.001;
			ws = false;
			@SuppressWarnings("unchecked")
			LinkedList<TaskDesc> taskList = (LinkedList<TaskDesc>)msg.obj;
			int numTaskToExecute = numIdleCore <= msg.info ? numIdleCore : msg.info;
			for (int i = 0; i < numTaskToExecute; i++) {
				executeTask(taskList.poll());
			}
			numIdleCore -= numTaskToExecute;
			while (taskList.size() > 0)
				sharedReadyTaskPQ.add(taskList.poll());
		} else {
			Library.numFailWorkStealing++;
			localTime += pollInterval;
			pollInterval *= 2;
			if (pollInterval < Library.pollIntervalUB) {
				if (!ws && Library.numComputeNode > 1)
				{
					ws = true;
					Message workstealMsg = new Message("work steal", -1, null, 
						null, localTime, id, id, Library.eventId++);
					SimMatrix.add(workstealMsg);
				}
			}
		}
	}
	
	public void procReqTaskEvent(Message msg) {
		schedMaxFwdTime = Library.updateTime(Library.unpackOverhead, schedMaxFwdTime);
		Message sendTaskMsg = new Message("send task", 0, null, 
				null, 0, id, msg.destId, Library.eventId++);
		
		int numTaskSent = sharedReadyTaskPQ.size() / 2;
		if (numTaskSent > 0) {
			numTaskSent += numTaskSent;
			Library.numStealTask += numTaskSent;
			LinkedList<TaskDesc> taskList = new LinkedList<TaskDesc>();
			for (int i = 0; i < numTaskSent; i++)
				taskList.add(sharedReadyTaskPQ.poll());
			sendTaskMsg.info = numTaskSent;
			sendTaskMsg.obj = taskList;
		}
		schedMaxFwdTime = Library.updateTime(Library.packOverhead, schedMaxFwdTime);
		sendMsg(sendTaskMsg, schedMaxFwdTime);
	}
}