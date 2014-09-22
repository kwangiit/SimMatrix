/**
 *
 * @author Ke Wang Since June 2011
 */

/*
 * this class is the compute node of the distributed simulator
 */

import java.util.*;

public class Scheduler
{
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

    public Scheduler(int id, int numCore, int numNeigh)
    {
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
    public void selectNeigh(boolean[] target)
    {
        for(int i = 0; i < Library.numNeigh; i++)
        {
            neighId[i] = (int)(Math.random() * Library.numComputeNode);

            /*
             * if the chosen node is itself, or has already
             * been chosen, then choose again
             */
            while(neighId[i] == id || target[neighId[i]])
            {
                neighId[i] = (int)(Math.random() * Library.numComputeNode);
            }
            target[neighId[i]] = true;
        }
    }

    /*
     * reset the boolean flags to be all false in case
     * to choose neighbors next time
     */
    public void resetTarget(boolean[] target)
    {
        for(int i = 0; i < Library.numNeigh; i++)
        {
            target[neighId[i]] = false;
        }
    }

    /* execute a task */
    public void executeTask(double length, double simuTime)
    {
        /* insert a task end event to be processed later */
        Event taskEnd = new Event((byte) 2, -1, simuTime + length,
                            id, id, Library.eventId++);
        DistributedSimulator.add(taskEnd);
        Library.numBusyCore++;
        Library.waitQueueLength--;
    }

    /* execute tasks */
    public void execute(double simuTime)
    {
        int numTaskToExecute = numIdleCore;
        if (readyTaskListSize < numTaskToExecute)
        {
            numTaskToExecute = readyTaskListSize;
        }
        double length = 0;
        for (int i = 0; i < numTaskToExecute; i++)
        {
            /* generate the task length with
             * uniform random distribution, raning from
             * [0, Library.maxTaskLength)
             */
            length = Math.random() * Library.maxTaskLength;
            this.executeTask(length, simuTime);
        }
        numIdleCore -= numTaskToExecute;
        readyTaskListSize -= numTaskToExecute;
    }

    /* poll the neighbors to get the load information to steal tasks */
    public void askLoadInfo(Scheduler[] nodes)
    {
        int maxLoad = -Library.numCorePerNode;
        int curLoad = 0;
        int maxLoadNodeId = 0;
        double totalLat = 0.0;
        selectNeigh(Library.target);
        resetTarget(Library.target);
        for (int i = 0; i < Library.numNeigh; i++)
        {
            curLoad = nodes[neighId[i]].readyTaskListSize - nodes[neighId[i]].numIdleCore;
            if (curLoad > maxLoad)
            {
                maxLoad = curLoad;
                maxLoadNodeId = nodes[neighId[i]].id;
            }
        }

        /* if the most heavist loaded neighbor has more available tasks */
        if (maxLoad > 1)
        {
            // latency due to asking and receiving Load info, and to the request of Jobs
            totalLat = (2 * Library.numNeigh + 1) * Library.stealMsgCommTime;
            Library.numMsg = Library.numMsg + 2 * Library.numNeigh + 1;

            // send a message to requst tasks
            Event requestTask = new Event((byte)4, -1, DistributedSimulator.getSimuTime() +
                    totalLat, id, maxLoadNodeId, Library.eventId++);
            DistributedSimulator.add(requestTask);
            Library.numWorkStealing++;
            // set the poll interval back to 0.01
            pollInterval = 0.01;
        } 
        else    // no neighbors have more available tasks
        {
            totalLat = 2 * Library.numNeigh * Library.stealMsgCommTime;
            Library.numMsg += 2 * Library.numNeigh;
            
            totalLat += pollInterval;   // wait for poll interval time to do work stealing againg
            pollInterval *= 2;   // double the poll interval
            Event stealEvent = new Event((byte)3, -1, DistributedSimulator.getSimuTime() +
                                totalLat, id, -2, Library.eventId++);
            DistributedSimulator.add(stealEvent);
        }
    }
    
    public void sendEvent(Event event, double time) {
    	byte[] msgByte = Library.serialize(event);
    	event.occurTime = time + Library.getCommOverhead(msgByte.length);
    	DistributedSimulator.add(event);
    }
    
    public void checkReadyTask() {
    	Integer taskId = waitTaskList.pollFirst();
    	if (taskId != null)
    	{
    		int kvsServerId = Library.hashServer(taskId);
    		Event checkReadyEvent = new Event((byte)1, taskId, null, 
    			0, id, kvsServerId, Library.eventId++);
    		schedMaxFwdTime = Library.updateTime(Library.packOverhead, schedMaxFwdTime);
    		if (kvsServerId == id) {
    			checkReadyEvent.occurTime = schedMaxFwdTime;
    			DistributedSimulator.add(checkReadyEvent);
    		} else {
    			schedMaxFwdTime = Library.updateTime(Library.packOverhead, schedMaxFwdTime);
    			sendEvent(checkReadyEvent, schedMaxFwdTime);
    		}
    	}
    }
    
    public void procCheckReadyTaskEvent(Event event) {
    	kvsMaxFwdTime = Library.updateTime(Library.unpackOverhead, kvsMaxFwdTime);
    	kvsMaxProcTime = Library.timeCompareOverried(kvsMaxProcTime, kvsMaxFwdTime);
    	kvsMaxProcTime = Library.updateTime(
    			Library.procTimePerKVSRequest, kvsMaxProcTime);
    	TaskMetaData taskMD = (TaskMetaData)(kvsHM.get(event.obj));
    	Event resCheckReadyTask = new Event((byte)2, event.info, 
    			taskMD, 0, id, event.sourceId, Library.eventId++);
    	kvsMaxFwdTime = Library.timeCompareOverried(kvsMaxFwdTime, kvsMaxProcTime);
    	kvsMaxFwdTime = Library.updateTime(Library.packOverhead, kvsMaxFwdTime);
    	if (event.sourceId == event.destId) {
    		resCheckReadyTask.occurTime = kvsMaxFwdTime;
    		DistributedSimulator.add(resCheckReadyTask);
    	} else
    		sendEvent(resCheckReadyTask, kvsMaxFwdTime);
    }
    
    public int taskReadyProcess(TaskMetaData taskMD, TaskDesc td, double time) {
    	int flag = 2;
    	if (taskMD.allDataSize <= Library.dataSizeThreshold) {
    		td.dataLength = taskMD.allDataSize;
    		flag = 0;
    	}
    	else {
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
    				schedMaxFwdTime = Library.updateTime(time - DistributedSimulator.
    						getSimuTime() + Library.packOverhead, schedMaxFwdTime);
    				Event pushTaskEvent = new Event((byte)4, td.taskId, 
    						td, 0, id, maxDataSchedId, Library.eventId++);
    				sendEvent(pushTaskEvent, schedMaxFwdTime);
    				flag = 2;
    			}
    		}
    	}
    	return flag;
    }
    
    public void executeTask(TaskDesc td) {
    	
    }
    
    public void procResCheckReadyTaskEvent(Event event) {
    	double time = DistributedSimulator.getSimuTime() + 
    			Library.unpackOverhead + Library.procTimePerTask;
    	TaskMetaData taskMD = (TaskMetaData)(event.obj);
    	if (taskMD.indegree > 0) 
    		waitTaskList.add(event.info);
    	else {
    		TaskDesc td = new TaskDesc();
    		td.taskId = taskMD.taskId;
    		int flag = taskReadyProcess(taskMD, td, time);
    		if (flag == 0 || flag == 1) {
    			if (numIdleCore > 0) {
    				numIdleCore--;
    				/*Event TaskDoneEvent = new Event((byte)5, td.taskId, td, time + 
    						Math.random() * Library.maxTaskLength, nodeId, 
    						nodeId, Library.eventId++);
    				DistributedSimulator.add(TaskDoneEvent);*/
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