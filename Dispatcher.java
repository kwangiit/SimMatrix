/**
 *
 * @author Ke Wang Since June 2011
 */

/*
 * this class is the centralized dispatcher of the centralized simulator
 */

import java.io.IOException;
import java.util.*;

public class Dispatcher
{
    int dispatcherId;
    int nextNodeToDispatch;
    long readyTaskListSize;   // wait queue lenghth
    long taskListSizeLowBd;   // the lower threshold of number of waiting tasks
    HashSet<Integer>[] hm;    // load information bucket

    @SuppressWarnings("unchecked")
    public Dispatcher(int dispatcherId)
    {
        this.dispatcherId = dispatcherId;
        nextNodeToDispatch = -1;
        this.readyTaskListSize = 0;
        this.taskListSizeLowBd = 200000;
        this.hm = new HashSet[Library.numCorePerNode + 1];
        for (int i = 0; i <= Library.numCorePerNode; i++)
        {
            hm[i] = new HashSet<Integer>(Library.numComputeNode);
        }

        /* at begining, all compute nodes have 0 load */
        for (int i = 0; i < Library.numComputeNode; i++)
        {
            hm[0].add(Library.hsInt[i]);
        }
    }

    /* update the load information of a node */
    public void updateHashMap(ComputeNode cn, int numChanged)
    {
    	int load = cn.numCores - cn.numIdleCores;
        hm[load].remove(Library.hsInt[cn.nodeId]);
        hm[load - numChanged].add(Library.hsInt[cn.nodeId]);
        cn.numIdleCores += numChanged;
    }

    /* find the next node to dipatch tasks */
    public int findNextNodeToDispatch()
    {
        /* if all cores of all nodes are occupied, then no idle compute nodes */
    	if (hm[Library.numCorePerNode].size() == Library.numComputeNode)
    	{
            return -1;
    	}
    	for (int i = 0; i < Library.numCorePerNode; i++)
    	{
            /* if some nodes have idle cores, return the first node */
            if (!hm[i].isEmpty())
            {
    		return hm[i].iterator().next();
            }
    	}
    	return -1;
    }

    /* calculate the communication time of numTaskToDispatch tasks*/
    public double pushTime(int numTaskToDispatch)
    {
        double msgSize = 0;
        msgSize = (double)Library.oneMsgSize * (double)numTaskToDispatch;
        return msgSize * 8 / Library.linkSpeed + Library.netLat;
    }

    /* dispatch tasks to compute node */
    public void doDispatch(ComputeNode recNode, int numTaskToDispatch)
    {
        double transmitTime = pushTime(numTaskToDispatch);
        double arriveTime = CentralSimulator.getSimuTime() + transmitTime;
        for (int i = 0; i < numTaskToDispatch; i++)
        {
            /* insert a task end event to be processed later,
             * task length is generated with uniform random distribution ranging
             * from [0, Library.maxTaskLength)
             */
            CentralSimulator.add(new Event((byte)2, -2, arriveTime + Math.random() * 
                                    Library.maxTaskLength + Library.oneMsgCommTime,
                                    recNode.nodeId, dispatcherId, Library.eventId++));
        }

        /* change the load information of the node */
        updateHashMap(recNode, -numTaskToDispatch);

        Library.numBusyCore += numTaskToDispatch;
        readyTaskListSize -= numTaskToDispatch;
    }

    /* advance the simulation time to the current event's occurrence time*/
    public void advanceTime(Event event)
    {
    	CentralSimulator.setSimuTime(event.occurTime);
	CentralSimulator.remove(event); // delete the first event
    }

    /* dispatch receives tasks from client */
    public void submissionEventProcess(long numTask)
    {
        readyTaskListSize += numTask;
    }

    /* summary logging event processing */
    public void logEventProcess()
    {
        Library.printSummaryLog(readyTaskListSize);
        Event logging = new Event((byte)1, 0, CentralSimulator.getSimuTime()
                + Library.logTimeInterval, -3, -3, Library.eventId++);
        if (Library.numTaskFinished != Library.numAllTask)
        {
            CentralSimulator.add(logging);
        }
    }

    /* task end event processing */
    public void taskEndEventProcess(ComputeNode cn)
    {
    	Library.numBusyCore--;
    	Library.numTaskFinished++;
    	updateHashMap(cn, 1);
    }

    /* dispatcher keeps dispatching tasks to compute nodes */
    public void dispatchToComputeNode(ComputeNode[] nodes, Client client)
    {
    	int numTaskToDispatch = 0;
    	double nextSimuTime = 0.0;
    	
    	while(readyTaskListSize != 0 || !CentralSimulator.isEmpty())
    	{
            nextNodeToDispatch = findNextNodeToDispatch();
            Event event = CentralSimulator.getFirst();
    		
            /* If all cores are busy */
            if (nextNodeToDispatch == -1)
            {
                if (event != null)
                {
                    while (event.type == 0 || event.type == 1)
                    {
                        advanceTime(event);
    			if (event.type == 0)
    			{
                            submissionEventProcess(event.info);
    			}
    			else
    			{
                            logEventProcess();
    			}
    			if ((event = CentralSimulator.getFirst()) == null)
    			{
                            break;
    			}
                    }
                    if (event == null)
                    {
    			return;
                    }
		
                    /* Until there is some task finished, meaning a core is available*/
                    advanceTime(event);
                    taskEndEventProcess(nodes[event.sourceId]);
                    nextNodeToDispatch = event.sourceId;
                    if ((event = CentralSimulator.getFirst()) == null)
                    {
                        return;
                    }
                }
    		else
    		{
                    return;
    		}
            }
    	
            /* When the dispatcher is waiting for client submitting tasks*/
            if ((event.type == 0 || event.type == 1) && readyTaskListSize == 0)
            {
    		advanceTime(event);
		if (event.type == 0)
		{
                    submissionEventProcess(event.info);
		}
		else
		{
                    logEventProcess();
		}
		event = CentralSimulator.getFirst();
            }
    	
            /* Dispatch tasks */
            numTaskToDispatch = nodes[nextNodeToDispatch].numIdleCores;
            if (readyTaskListSize < numTaskToDispatch)
            {
    		numTaskToDispatch = (int)readyTaskListSize;
            }
            if (numTaskToDispatch == 0)
            {
                continue;
            }
            nextSimuTime = CentralSimulator.getSimuTime() +
                            Library.procTimePerTask * numTaskToDispatch;
            while (event != null && event.occurTime <= nextSimuTime)
            {
                advanceTime(event);
                if (event.type == 0)
    		{
                    submissionEventProcess(event.info);
    		}
    		else if (event.type == 1)
    		{
                    logEventProcess();
    		}
    		else
    		{
                    taskEndEventProcess(nodes[event.sourceId]);
    		}
                event = CentralSimulator.getFirst();
            }
            CentralSimulator.setSimuTime(nextSimuTime);
            doDispatch(nodes[nextNodeToDispatch], numTaskToDispatch);
            if (readyTaskListSize == 0 && client.numLeftTasks == 0)
            {
    		break;
            }
            else if (readyTaskListSize < taskListSizeLowBd && client.numLeftTasks > 0)
            {
    		client.submitTaskToDispatcher(CentralSimulator.getSimuTime(), dispatcherId, true);
            }
    	}

        /* after client submits all the tasks */
    	while (!CentralSimulator.isEmpty())
    	{
            Event event = CentralSimulator.pollFirst();
            CentralSimulator.setSimuTime(event.occurTime);
            if(event.type == 1)
            {
               logEventProcess();
            }
            else
            {
            	taskEndEventProcess(nodes[event.sourceId]);
            }
    	}

        /* flush the summary log */
    	try
    	{
            Library.logBuffWriter.flush();
            Library.logBuffWriter.close();
    	}
    	catch (IOException e)
    	{
            e.printStackTrace();
    	}  
    }
}