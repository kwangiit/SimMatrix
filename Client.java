/**
 *
 * @author Ke Wang Since June 2011
 */

/*
 * this class is the Client who can submit tasks
 */

public class Client
{
    int clientId;
    long numLeftTasks;
    long numTaskRecv;
    boolean waitFlag;

    public Client(int clientId, long numLeftTasks, long numTaskRecv)
    {
        this.clientId = clientId;
        this.numLeftTasks = numLeftTasks;
        this.numTaskRecv = numTaskRecv;
        this.waitFlag = false;
    }

    /* client submits tasks to the dispatcher */
    public void submitTaskToDispatcher(double simuTime, int dispatcherId, boolean cs)
    {
        double msgSize = 0;
        double submitLat = 0;
        long numTaskToSubmit = Library.numTaskToSubmit;
        if (numTaskToSubmit > numLeftTasks)
        {
            numTaskToSubmit = (int)numLeftTasks;
        }
        if (numTaskToSubmit == 0)
        {
            return;
        }
        msgSize = (double)numTaskToSubmit * (double)Library.taskSize;
        submitLat = msgSize * 8 / Library.linkSpeed + Library.netLat;
        Event submission = new Event((byte)0, numTaskToSubmit, simuTime +
                                        submitLat, clientId, dispatcherId, Library.eventId++);

        /* if it is the centralized engine, insert submission event to centralized global queue
         * otherwise, insert submission event to the distribute global queue
         */
        if (cs)
        {
            CentralSimulator.add(submission);
        }
        else
        {
            DistributedSimulator.add(submission);
            waitFlag = true;
        }
        Library.numTaskSubmitted += numTaskToSubmit;
        numLeftTasks -= numTaskToSubmit;
    }
}