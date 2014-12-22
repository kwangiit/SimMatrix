/**
 *
 * @author Ke Wang Since June 2011
 */

/*
 * this is the library class which defines all the global parameters
 */

import java.io.*;
import java.util.*;

public class Library {
	public static int numComputeNode; // No. of Compute Nodes of the systems
	public static int numCorePerNode; // No. of cores per node
	public static int[] nodeIds;

	public static int numTaskPerCore;
	public static int numAllTask; // Total no. of jobs
	public static double maxTaskLength;
	public static HashMap<Integer, Task> globalTaskHM;
	public static String dagType;
	public static int dagPara;

	public static double networkBandwidth; // The network link speed
	public static double networkLatency; // The network latency
	public static int singleMsgSize; // Job description size
	public static double singleMsgTransTime;
	public static double packOverhead;
	public static double unpackOverhead;
	//public static double stealMsgCommTime;
	
	public static double procTimePerTask; // The processing time per job
	public static double procTimePerKVSRequest;

	public static BufferedWriter summaryLogBW = null; // Summary log writer
	public static double logTimeInterval; // The interval to do logging
	public static boolean taskLog; // Log for each task or not
	public static BufferedWriter taskLogBW = null; // Task log writer
	
	public static double visualTimeInterval;
	public static double screenCapMilInterval;

	public static long eventId;
	public static int numTaskFinished;
	public static long numMsg;
	public static double ratioThreshold;
	public static double localQueueTimeThreshold;
	
	public static int numNeigh;
	//public static double infoMsgSize;
	public static boolean target[];
	public static double initPollInterval;
	public static double pollIntervalUB;
	public static long numStealTask;
	public static long numWorkStealing;
	public static long numFailWorkStealing;

	public static Runtime runtime;
	public static double currentSimuTime;
	public static String numUser;
	public static String numResource;
	public static int numThread;
	public static int numAllCore;
	public static int numFreeCore;
	public static String numPendCore;
	public static int numBusyCore;
	public static int waitQueueLength;
	public static int readyQueueLength;
	public static int activeQueueLength;
	public static int doneQueueLength;
	public static int deliveredTask;
	public static int oldDeliveredTask;
	public static double throughput;
	public static int successTask;
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

	//public static Integer[] hsInt;
	//public static boolean[] target;

	/* write to the summary log */
	public static void summaryLogging() {
		currentSimuTime = SimMatrix.getSimuTime();
		numThread = Thread.activeCount();
		numFreeCore = numAllCore - numBusyCore;
		//waitQueueLength = readyTaskListSize;
		activeQueueLength = numBusyCore;
		deliveredTask = numTaskFinished;
		doneQueueLength = numTaskFinished;
		throughput = (deliveredTask - oldDeliveredTask)/ Library.logTimeInterval;
		successTask = deliveredTask;
		jvmSize = runtime.totalMemory();
		jvmFreeSize = runtime.freeMemory();
		if (jvmSize > jvmMaxSize)
			jvmMaxSize = jvmSize;
		String line = Double.toString(currentSimuTime) + " " + numUser + " "
				+ numResource + " " + Integer.toString(numThread) + " "
				+ Integer.toString(numAllCore) + " " + Integer.toString(numFreeCore)
				+ " " + numPendCore + " " + Integer.toString(numBusyCore) + " "
				+ Integer.toString(waitQueueLength) + " " + Integer.toString(readyQueueLength)
				+ " " + Integer.toString(activeQueueLength) + " "
				+ Integer.toString(doneQueueLength) + " " + Long.toString(deliveredTask) + " "
				+ Double.toString(throughput) + " "
				+ Integer.toString(successTask) + " " + failedTask + " "
				+ retriedTask + " " + resourceAllocated + " "
				+ Integer.toString(Library.numAllTask) + " " + cacheSize + " " + cacheLocalHit
				+ " " + cacheGlobalHit + " " + cacheMiss + " "
				+ cacheLocalHitRatio + " " + cacheGlobalHitRatio + " "
				+ systemCPUUser + " " + systemCPUSystem + " " + systemCPUIdle
				+ " " + Long.toString(jvmSize) + " "
				+ Long.toString(jvmFreeSize) + " " + jvmMaxSize + "\n";
		try {
			summaryLogBW.write(line);
		} catch (IOException e) {
			System.out.println(e);
			System.out.println("What is the matter?");
			System.exit(1);
		}
		oldDeliveredTask = deliveredTask;
	}
	
	public static double getCommOverhead(int msgSize) {
		return msgSize * 8.0 / (double) Library.networkBandwidth + Library.networkLatency;
	}
	
	public static double updateTime(double increment, double base) {
		double cur = SimMatrix.getSimuTime();
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
		return Library.nodeIds[hashCode % Library.numComputeNode];
	}
	
	public static byte[] serialize(Object obj) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(obj);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return baos.toByteArray();		
	}
	
	/* summary logging event processing */
	public static void procLoggingEvent() {
		Library.summaryLogging();
		Message logging = new Message("logging", -1, null, null,
				SimMatrix.getSimuTime() + Library.logTimeInterval,
				-2, -2, Library.eventId++);
		SimMatrix.add(logging);
	}
	
	public static void procLoggingTaskEvent() {
		if (Library.taskLog) {
			try 
			{
				for (int i = 0; i < Library.numAllTask; i++) {
					Task task = Library.globalTaskHM.get(i);
					try{
						taskLogBW.write(task.taskId + " " + task.dataOutPutSize + " " + 
								task.submissionTime + " " + task.readyTime + " " + 
								task.startExecTime + " " + task.endTime + "\n");
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				}
				taskLogBW.flush();
				taskLogBW.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}