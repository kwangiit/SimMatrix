/**
 *
 * @author Ke Wang Since June 2011
 */

/*
 * this class is the centralzied simulator engine
 */

import java.io.*;
import java.util.*;

public class CentralSimulator 
{
    private static double simuTime;          // simulation time
    private static TreeSet<Event> ts;        // global event queue

    Client client;   // a client
    Dispatcher dispatcher; // centralized dispatcher
    ComputeNode[] nodes;   // all the compute nodes

    double throughput;

    /* static methods to operate the simulation time and event queue */
    public static void setSimuTime(double time)
    {
        CentralSimulator.simuTime = time;
    }

    public static double getSimuTime()
    {
        return CentralSimulator.simuTime;
    }

    public static void add(Event event)
    {
        CentralSimulator.ts.add(event);
    }

    public static Event getFirst()
    {
    	if (CentralSimulator.isEmpty())
    	{
    		return null;
    	}
    	return CentralSimulator.ts.first();
    }
    
    public static void remove(Event event)
    {
    	CentralSimulator.ts.remove(event);
    }
    
    public static boolean isEmpty()
    {
    	return CentralSimulator.ts.isEmpty();
    }
    
    public static Event pollFirst()
    {
    	return CentralSimulator.ts.pollFirst();
    }
    
    public static int size()
    {
    	return CentralSimulator.ts.size();
    }

    /* initialize the parameters from the library */
    public void initLibrary(String[] args)
    {
        Library.numComputeNode = Integer.parseInt(args[0]);
        Library.numCorePerNode = Integer.parseInt(args[1]);
        Library.numTaskPerCore = Integer.parseInt(args[2]);
        Library.maxTaskLength = Double.parseDouble(args[3]);

        Library.linkSpeed = 6800000000.0;    // b/sec
        Library.netLat = 0.0001;            // second
        Library.procTimePerTask = 0.001;    // second

        Library.taskSize = 1024.0;    // Bytes
        Library.numAllTask = (long)Library.numComputeNode *
                (long)Library.numCorePerNode * (long)Library.numTaskPerCore;
        Library.taskLog = false;    // log for each task or not
        if (Library.numComputeNode < 8192)
        {
            Library.logTimeInterval = 1;  // the interval to do logging
        }
        else
        {
            Library.logTimeInterval = (int)(Library.numComputeNode / 4096);
        }
        Library.numTaskToSubmit = 1000000;
        Library.numTaskSubmitted = 0;
        Library.numTaskFinished = 0;
        Library.eventId = 0;
        
        Library.oneMsgCommTime = (double)Library.taskSize * 8 /
                (double)Library.linkSpeed + Library.netLat;
        try
        {
            Library.logBuffWriter = new BufferedWriter(new FileWriter(
                    "summary_" +  Library.numComputeNode + ".txt"));
            Library.taskBuffWriter = new BufferedWriter(new FileWriter(
                    "task_" +  Library.numComputeNode + ".txt"));
        }
        catch(IOException ioe)
        {
            ioe.printStackTrace();
        }

        Library.runtime = Runtime.getRuntime();
        Library.numUser = "1";
        Library.numResource = "0";
        Library.numAllCore = (long)Library.numComputeNode *
                                        (long)Library.numCorePerNode;
        Library.numBusyCore = 0;
        Library.numPendCore = "0";
        Library.waitNotQueueLength = "0";
        Library.doneQueueLength = "0";
        Library.failedTask = "0";
        Library.retriedTask = "0";
        Library.resourceAllocated = "0";
        Library.cacheSize = "0";
        Library.cacheLocalHit = "0";
        Library.cacheGlobalHit = "0";
        Library.cacheMiss = "0";
        Library.cacheLocalHitRatio = "0";
        Library.cacheGlobalHitRatio = "0";
        Library.systemCPUUser = "0";
        Library.systemCPUSystem = "0";
        Library.systemCPUIdle = "100";
        Library.oldDeliveredTask = 0;

        Library.hsInt = new Integer[Library.numComputeNode];
        for (int i = 0; i < Library.numComputeNode; i++)
        {
            Library.hsInt[i] = Integer.valueOf(i);
        }
    }

    /* initialization of the simulation environment */
    public void initSimulation(String[] args)
    {
        System.out.println("Initializing......");
        initLibrary(args);
        CentralSimulator.setSimuTime(0);
        CentralSimulator.ts = new TreeSet<Event>();
        client = new Client(-2, Library.numAllTask, 0);
        dispatcher = new Dispatcher(-1);
        nodes = new ComputeNode[Library.numComputeNode];
        for (int i = 0;i < Library.numComputeNode;i++)
        {
            nodes[i] = new ComputeNode(i, Library.numCorePerNode,
                                                Library.numCorePerNode);

        }
        throughput = 0;
        System.out.println("Finish Initializing!");
    }

    /* print the reslt */
    public void outputResult(long start, long end)
    {
        System.out.println("Number of compute node is:" +
                Library.numComputeNode);
        System.out.println("Simulation time is:" +
                CentralSimulator.getSimuTime());
        throughput = (double)(Library.numAllTask) /
                CentralSimulator.getSimuTime();
        System.out.println("Througput is:" + throughput);
        System.out.println("Real CPU time is: " +
                (double)(end - start) / 1000 + " s\n");
    }

    public static void main(String[] args)
    {
        if (args.length != 4)
        {
            System.out.println("Need three parameters: number of nodes, "
                    + "number of cores per node, number of tasks per core, max task length");
            System.exit(1);
        }
        long start = System.currentTimeMillis();
        CentralSimulator cs = new CentralSimulator();
        cs.initSimulation(args);

        /* add first logging event at time 0 */
        Event logging = new Event((byte)1, -1, CentralSimulator.getSimuTime(),
                                    -3, -3, Library.eventId++);
        CentralSimulator.add(logging);

        // client submits tasks at time 0
        cs.client.submitTaskToDispatcher(CentralSimulator.getSimuTime(),
                cs.dispatcher.dispatcherId, true);

        // dispatcher starts to work
        cs.dispatcher.dispatchToComputeNode(cs.nodes, cs.client);
        long end = System.currentTimeMillis();
        cs.outputResult(start,end);
    }
}