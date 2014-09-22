/**
 *
 * @author Ke Wang Since June 2011
 */

/*
 * this is the library class which defines all the global parameters
 */

import java.io.*;

public class Library {
	public static int numComputeNode; // No. of Compute Nodes of the systems
	public static int[] nodeId;
	public static int numCorePerNode; // No. of cores per node
	public static int numTaskPerCore;
	public static String dagType;
	public static int dagPara;

	public static double linkSpeed; // The network link speed
	public static double netLat; // The network latency
	public static double oneMsgCommTime;
	public static double stealMsgCommTime;
	public static double procTimePerTask; // The processing time per job
	public static double procTimePerKVSRequest;
	public static double packOverhead;
	public static double unpackOverhead;

	public static int oneMsgSize; // Job description size
	public static double maxTaskLength;
	public static int numAllTask; // Total no. of jobs
	public static boolean taskLog; // Log for each task or not
	public static double logTimeInterval; // The interval to do logging
	public static double visualTimeInterval;
	public static double screenCapMilInterval;

	public static long numTaskToSubmit;
	public static long numTaskLowBd;
	public static long numTaskSubmitted;
	public static long numTaskFinished;
	public static long eventId;

	public static int numNeigh;
	public static double infoMsgSize;
	public static long numStealTask;
	public static BufferedWriter logBuffWriter = null; // Summary log writer
	public static BufferedWriter taskBuffWriter = null; // Task log writer

	public static Runtime runtime;
	public static double currentSimuTime;
	public static String numUser;
	public static String numResource;
	public static int numThread;
	public static long numAllCore;
	public static long numFreeCore;
	public static String numPendCore;
	public static long numBusyCore;
	public static long waitQueueLength;
	public static String waitNotQueueLength;
	public static long activeQueueLength;
	public static String doneQueueLength;
	public static long deliveredTask;
	public static long oldDeliveredTask;
	public static double throughput;
	public static long successTask;
	public static String failedTask;
	public static String retriedTask;
	public static String resourceAllocated;
	public static String cacheSize;
	public static String cacheLocalHit;
	public static String cacheGlobalHit;
	public static String cacheMiss;
	public static String cacheLocalHitRatio;
	public static String cacheGlobalHitRatio;
	public static String systemCPUUser;
	public static String systemCPUSystem;
	public static String systemCPUIdle;
	public static long jvmSize;
	public static long jvmFreeSize;
	public static long jvmMaxSize;

	public static Integer[] hsInt;
	public static boolean[] target;

	public static long numMsg;
	public static long numWorkStealing;
	public static long numFailWorkStealing;
	
	public static int dataSizeThreshold;

	/* write to the summary log */
	public static void printSummaryLog(long readyTaskListSize) {
		currentSimuTime = CentralSimulator.getSimuTime();
		numThread = Thread.activeCount();
		numFreeCore = numAllCore - numBusyCore;
		waitQueueLength = readyTaskListSize;
		activeQueueLength = numBusyCore;
		deliveredTask = numTaskFinished;
		throughput = (deliveredTask - oldDeliveredTask)
				/ Library.logTimeInterval;
		successTask = deliveredTask;
		jvmSize = runtime.totalMemory();
		jvmFreeSize = runtime.freeMemory();
		if (jvmSize > jvmMaxSize) {
			jvmMaxSize = jvmSize;
		}
		String line = Double.toString(currentSimuTime) + " " + numUser + " "
				+ numResource + " " + Integer.toString(numThread) + " "
				+ Long.toString(numAllCore) + " " + Long.toString(numFreeCore)
				+ " " + numPendCore + " " + Long.toString(numBusyCore) + " "
				+ Long.toString(waitQueueLength) + " " + waitNotQueueLength
				+ " " + Long.toString(activeQueueLength) + " "
				+ doneQueueLength + " " + Long.toString(deliveredTask) + " "
				+ Double.toString(throughput) + " "
				+ Long.toString(successTask) + " " + failedTask + " "
				+ retriedTask + " " + resourceAllocated + " "
				+ numTaskSubmitted + " " + cacheSize + " " + cacheLocalHit
				+ " " + cacheGlobalHit + " " + cacheMiss + " "
				+ cacheLocalHitRatio + " " + cacheGlobalHitRatio + " "
				+ systemCPUUser + " " + systemCPUSystem + " " + systemCPUIdle
				+ " " + Long.toString(jvmSize) + " "
				+ Long.toString(jvmFreeSize) + " " + jvmMaxSize + "\r\n";
		try {
			logBuffWriter.write(line);
		} catch (IOException e) {
			System.out.println(e);
			System.exit(1);
			System.out.println("What is the matter?");
		}
		oldDeliveredTask = deliveredTask;
	}
	
	public static double getCommOverhead(int msgSize) {
		return msgSize * 8.0 / (double) Library.linkSpeed + Library.netLat;
	}
	
	public static double updateTime(double increment, double base) {
		double cur = DistributedSimulator.getSimuTime();
		if (cur > base)
			base = cur;
		base += increment;
		return base;
	}
	
	public static double timeCompareOverried(double src, double dest) {
		if (src < dest)
			src = dest;
		return src;
	}
	
	public static int hashServer(Object key) {
		int hashCode = Math.abs(key.hashCode());
		return Library.nodeId[hashCode % Library.numComputeNode];
	}
	
	public static byte[] serialize(Object obj) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try
        {
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(obj);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return baos.toByteArray();		
	}
	
	/* summary logging event processing */
	public static void loggingEventProcess() {
		Library.printSummaryLog(Library.waitQueueLength);
		Event logging = new Event((byte) 0, -1, null, 
				DistributedSimulator.getSimuTime() + Library.logTimeInterval,
				-2, -2, Library.eventId++);
		DistributedSimulator.add(logging);
	}
}