/**
 *
 * @author Ke Wang Since June 2011
 */

/*
 * this class is the compute node of the distributed simulator
 */

public class ComputeNodeD
{
    int nodeId;
    int numCores;
    int numIdleCores;
    int[] neighId;
    int readyTaskListSize;  // ready queue size
    int numTaskDispatched;
    int numTaskFinished;
    double pollInterval;

    public ComputeNodeD(int nodeId, int numCores, int numNeigh)
    {
        this.nodeId = nodeId;
        this.numCores = numCores;
        this.numIdleCores = numCores;
        this.neighId = new int[numNeigh];
        this.readyTaskListSize = 0;
        this.numTaskDispatched = 0;
        this.numTaskFinished = 0;
        this.pollInterval = 0.01;
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
            while(neighId[i] == nodeId || target[neighId[i]])
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
                            nodeId, nodeId, Library.eventId++);
        DistributedSimulator.add(taskEnd);
        Library.numBusyCore++;
        Library.waitQueueLength--;
    }

    /* execute tasks */
    public void execute(double simuTime)
    {
        int numTaskToExecute = numIdleCores;
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
        numIdleCores -= numTaskToExecute;
        readyTaskListSize -= numTaskToExecute;
    }

    /* poll the neighbors to get the load information to steal tasks */
    public void askLoadInfo(ComputeNodeD[] nodes)
    {
        int maxLoad = -Library.numCorePerNode;
        int curLoad = 0;
        int maxLoadNodeId = 0;
        double totalLat = 0.0;
        selectNeigh(Library.target);
        resetTarget(Library.target);
        for (int i = 0; i < Library.numNeigh; i++)
        {
            curLoad = nodes[neighId[i]].readyTaskListSize - nodes[neighId[i]].numIdleCores;
            if (curLoad > maxLoad)
            {
                maxLoad = curLoad;
                maxLoadNodeId = nodes[neighId[i]].nodeId;
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
                    totalLat, nodeId, maxLoadNodeId, Library.eventId++);
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
                                totalLat, nodeId, -2, Library.eventId++);
            DistributedSimulator.add(stealEvent);
        }
    }
}