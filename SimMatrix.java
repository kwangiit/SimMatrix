/**
 *
 * @author Ke Wang Since June 2011
 */

/*
 * this class is the distributed simulator engine
 */

import java.util.*;
import java.io.*;
import java.awt.*;

import javax.swing.*;

public class SimMatrix {
	private static double simuTime; // simulation time
	private static TreeSet<Message> ts; // global event queue

	Client client; // a client
	Scheduler[] schedulers; // all the compute nodes

	JFrame window; // the window to do visualization
	ModCanvas canvas; // the canvas for painting

	double throughput;

	/* static methods to operate the simulation time and event queue */
	public static void setSimuTime(double time) {
		SimMatrix.simuTime = time;
	}

	public static double getSimuTime() {
		return SimMatrix.simuTime;
	}

	public static boolean isEmpty() {
		return SimMatrix.ts.isEmpty();
	}

	public static void add(Message event) {
		SimMatrix.ts.add(event);
	}

	public static Message pollFirst() {
		return SimMatrix.ts.pollFirst();
	}

	/* initialize the parameters from the library */
	public void initLibrary(String[] args) throws IOException{
		BufferedReader configBR = new BufferedReader(new FileReader(args[0]));
		HashMap<String, String> configHM = new HashMap<String, String>();
		String str = configBR.readLine();
		while (str != null) {
			String[] strLine = str.split("\t");
			configHM.put(strLine[0], strLine[1]);
			str = configBR.readLine();
		}
		Library.numComputeNode = Integer.parseInt(configHM.get("NumComputeNode"));
		Library.nodeIds = new int[Library.numComputeNode];
		for (int i = 0; i < Library.numComputeNode; i++)
			Library.nodeIds[i] = i;
		Library.numCorePerNode = Integer.parseInt(configHM.get("NumCorePerNode"));
		Library.numTaskPerCore = Integer.parseInt(configHM.get("NumTaskPerCore"));
		Library.numAllTask = Library.numComputeNode * 
				Library.numCorePerNode * Library.numTaskPerCore;
		Library.maxTaskLength = Double.parseDouble(configHM.get("MaxTaskLength"));
		Library.dagType = configHM.get("DagType");
		Library.dagPara = Integer.parseInt(configHM.get("DagPara"));
		Library.networkBandwidth = Double.parseDouble(configHM.get("NetworkBandwidth")); // b/sec
		Library.networkLatency = Double.parseDouble(configHM.get("NetworkLatency")); // second
		Library.packOverhead = Double.parseDouble(configHM.get("PackOverhead"));
		Library.unpackOverhead = Double.parseDouble(configHM.get("UnPackOverhead"));
		Library.singleMsgSize = Integer.parseInt(configHM.get("SingleMsgSize")); // Bytes
		Library.singleMsgTransTime = Library.getCommOverhead(Library.singleMsgSize);
		Library.procTimePerTask = Double.parseDouble(configHM.get("ProcTimePerTask"));
		Library.procTimePerKVSRequest = Double.parseDouble(configHM.get("ProcTimePerKVSRequest"));
		
		Library.taskLog = Boolean.parseBoolean(configHM.get("TaskLog"));
		Library.eventId = 0;
		Library.numTaskFinished = 0;
		if (Library.numComputeNode == 1)
			Library.numNeigh = 0;
		else
			Library.numNeigh = (int) (Math.sqrt(Library.numComputeNode));

		//Library.infoMsgSize = 100; // Bytes
		//Library.stealMsgCommTime = (double) Library.infoMsgSize * 8 
		//		/ (double) Library.networkBandwidth + Library.networkLatency;
		//Library.numTaskSubmitted = 0;
		Library.numMsg = 0;
		Library.numWorkStealing = 0;
		Library.numFailWorkStealing = 0;
		Library.numStealTask = 0;
		
		Library.dataSizeThreshold = Integer.parseInt(configHM.get("DataSizeThreshold"));
		Library.initPollInterval = Double.parseDouble(configHM.get("InitPollInterval"));
		Library.pollIntervalUB = Double.parseDouble(configHM.get("PollIntervalUB"));
		
		Library.logTimeInterval = Double.parseDouble(configHM.get("LogTimeInterval"));//1.0;
		Library.visualTimeInterval = Double.parseDouble(configHM.get("VisualTimeInterval"));//0.5;
		
		Library.localQueueTimeThreshold = Double.parseDouble(
				configHM.get("LocalQueueTimeThreshold"));
		// equivalent to 20 frames per second
		Library.screenCapMilInterval = Integer.parseInt(configHM.get("ScreenCapMilInterval"));//50;

		
		Library.numThread = 1;
		Library.numAllCore = Library.numComputeNode * Library.numCorePerNode;
		Library.numFreeCore = Library.numAllCore;
		Library.numPendCore = "0";
		Library.numBusyCore = 0;
		Library.waitQueueLength = Library.numAllTask;
		Library.readyQueueLength = 0;
		Library.activeQueueLength = 0;
		Library.doneQueueLength = 0;
		Library.deliveredTask = 0;
		Library.oldDeliveredTask = 0;
		Library.throughput = 0.0;
		Library.successTask = 0;

		try {
			Library.summaryLogBW = new BufferedWriter(
					new FileWriter("summaryD_" + Library.numComputeNode));
		} catch (IOException e) {
			e.printStackTrace();
		}

		Library.runtime = Runtime.getRuntime();
		Library.numUser = "1";
		Library.numResource = "0";

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


		/* counters */
	}

	/* initialization of the simulation environment */
	public void initSimulation(String[] args) {
		System.out.println("Initializing...");
		initLibrary(args);
		SimMatrix.setSimuTime(0.0);
		SimMatrix.ts = new TreeSet<Message>();
		client = new Client(-1, Library.numAllTask);

		/* initialize nodes */
		schedulers = new Scheduler[Library.numComputeNode];
		for (int i = 0; i < Library.numComputeNode; i++) {
			schedulers[i] = new Scheduler(i);
		}

		System.out.println("Finish Initialization!");
	}

	/* initialization of the visualization window */
	public void initVisualization() {
		window = new JFrame("Visualization of Load");
		canvas = new ModCanvas((int) Math.sqrt(Library.numComputeNode), this);
		Container c = window.getContentPane();
		c.add(canvas);
		window.setVisible(true);
		window.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
		window.pack();
	}

	/*
	 * accept the submssion of tasks, either submitted by the client, or
	 * dispatched by the a neighbor in work stealing
	 */
//	public void taskReceptionEventProcess(Message event) {
//		Scheduler recvNode = schedulers[event.destId];
//		recvNode.readyTaskListSize += event.info;
//
//		/* if there are idle cores, then execute tasks */
//		if (recvNode.numIdleCore > 0) {
//			recvNode.execute(SimMatrix.getSimuTime());
//		}
//		if (event.sourceId == -1) {
//			Library.waitQueueLength += event.info;
//			client.waitFlag = false;
//		}
//	}

	/* task end event processing */
//	public void taskEndEventProcess(Message event) {
//		Scheduler curNode = schedulers[event.sourceId];
//		curNode.numIdleCore++;
//		curNode.numTaskFinished++;
//		Library.numTaskFinished++;
//		Library.numBusyCore--;
//		if (curNode.id == 0 && !client.waitFlag) {
//			/*
//			 * if the number of waiting tasks of the first node is below the
//			 * predefined threshold and the client still has tasks, then the
//			 * client would send more tasks
//			 */
//			if (client.numTask > 0
//					&& curNode.readyTaskListSize < Library.numTaskLowBd) {
//				client.submitTaskToDispatcher(
//						SimMatrix.getSimuTime()
//								+ Library.oneMsgCommTime, 0, false);
//			}
//		}
//
//		/*
//		 * if current node still have more tasks, then execute tasks, otherwise
//		 * do work tealing
//		 */
//		if (curNode.readyTaskListSize > 0) {
//			curNode.execute(SimMatrix.getSimuTime());
//		} else if (Library.numNeigh > 0) {
//			// instead of enqueueing one event, directly call the routine
//			curNode.askLoadInfo(schedulers);
//		}
//	}

	/* do work stealing */
//	public void stealEventProcess(Message event) {
//		Scheduler curNode = schedulers[event.sourceId];
//		// will chain with request or other steal events
//		curNode.askLoadInfo(schedulers);
//	}

	/* request task event processing */
//	public void taskDispatchEventProcess(Message event) {
//		Scheduler curNode = schedulers[event.destId];
//
//		/* send have of the load */
//		int loadToSend = (int) Math
//				.floor((curNode.readyTaskListSize - curNode.numIdleCore) / 2);
//
//		double latency = 0;
//		double msgSize = 0;
//
//		/* if current node has more available tasks, then send tasks */
//		if (curNode.readyTaskListSize >= loadToSend && loadToSend > 0) {
//			int nodeToSend = event.sourceId;
//			msgSize = loadToSend * Library.taskSize;
//			latency = msgSize * 8 / Library.linkSpeed + Library.netLat;
//
//			Message submission = new Message((byte) 0, loadToSend,
//					SimMatrix.getSimuTime() + latency,
//					curNode.id, nodeToSend, Library.eventId++);
//			Library.numMsg++;
//			SimMatrix.add(submission);
//			curNode.readyTaskListSize -= loadToSend;
//			curNode.numTaskDispatched += loadToSend;
//		} else // otherwise, ask the neighbor to steal again
//		{
//			loadToSend = 0;
//			// if it happens that the requested node didn't have any jobs
//			latency = Library.stealMsgCommTime;
//			Library.numMsg++;
//			Library.numFailWorkStealing++;
//			Message stealEvent = new Message((byte) 2, -1,
//					SimMatrix.getSimuTime() + latency,
//					event.sourceId, -2, Library.eventId++);
//			SimMatrix.add(stealEvent);
//		}
//		if (curNode.id == 0 && !client.waitFlag) {
//			if (client.numTask > 0
//					&& curNode.readyTaskListSize < Library.numTaskLowBd) {
//				client.submitTaskToDispatcher(
//						SimMatrix.getSimuTime(), 0, false);
//			}
//		}
//	}

	/* process the visualization event */
//	public void visualEvent() {
//		this.canvas.repaint();
//		Message visEvent = new Message(
//				(byte) 5,
//				-1,
//				SimMatrix.getSimuTime() + Library.visualTimeInterval,
//				-2, -2, Library.eventId++);
//		this.ts.add(visEvent);
//	}

	/*
	 * calculate the coefficient variance of the number of tasks finished by
	 * each node
	 */
	public double coVari() {
		double sum = 0;
		double mean;
		for (int i = 0; i < schedulers.length; i++) {
			sum += schedulers[i].numTaskFinished;
		}
		mean = sum / schedulers.length;
		sum = 0;
		for (int i = 0; i < schedulers.length; i++) {
			sum += Math.pow(schedulers[i].numTaskFinished - mean, 2);
		}
		double sd = Math.sqrt(sum / schedulers.length);
		if (mean == 0) {
			return 0;
		}
		return sd / mean;
	}

	/* print the result */
	public void outputResult(long start, long end) {
		System.out.println("Number of compute node is:"
				+ Library.numComputeNode);
		System.out.println("Simulation time is:"
				+ SimMatrix.getSimuTime());
		throughput = (double) (Library.numAllTask)
				/ SimMatrix.getSimuTime();
		System.out.println("Througput is:" + throughput);
		System.out.println("Real CPU time is: " + (double) (end - start) / 1000
				+ " s");
		System.out.println("The final coefficient of variation is:" + coVari());
		System.out.println("The number of Messages is:" + Library.numMsg);
		System.out.println("The number of Work Stealing is:"
				+ Library.numWorkStealing);
		System.out.println("The number of fail Work Stealing is:"
				+ Library.numFailWorkStealing);
		System.out.println();
	}

	public static void main(String[] args) throws InterruptedException {
		if (args.length != 1) {
			System.out.println("Please specify the configuration file!");
					//"Need three parameters: num_node, num_core_per_node, "
					//+ "num_tasks_per_core, max_task_length, dag_type, data_para");
			System.exit(1);
		}
		long start = System.currentTimeMillis();
		SimMatrix sm = new SimMatrix();
		sm.initSimulation(args);
		// ds.initVisualization();
		sm.client.genDagAdjlist(Library.dagType, Library.dagPara);
		sm.client.genDagIndegree();
		sm.client.insertTaskMetaToKVS(sm.schedulers);
		sm.client.splitTask(sm.schedulers);
		//ds.client.submitTaskToDispatcher(DistributedSimulator.getSimuTime(), 0, false);
		
		// at the beginning, add a logging event
		Message logging = new Message("logging", -1, null, null, 
				SimMatrix.getSimuTime(), -2, -2, Library.eventId++);
		SimMatrix.add(logging);
		
		/*
		 * first all the compute nodes check the waiting 
		 */
		for (int i = 0; i < Library.numComputeNode; i++) {
			sm.schedulers[i].localTime = SimMatrix.getSimuTime() + Math.random() * 0.001;
			sm.schedulers[i].checkReadyTask();
		}
		
		for (int i = 0; i < Library.numComputeNode; i++) {
			SimMatrix.add(new Message("work steal", -1, null, null, SimMatrix.getSimuTime() + 
					Math.random() * 0.001, i, -2, Library.eventId++));
			sm.schedulers[i].ws = true;
		}
		/*
		 * DistributedSimulator.add(new Event((byte)5, -1,
		 * DistributedSimulator.getSimuTime(), -1, -1, Library.eventId++));
		 */
		Message msg = null;
		while (!SimMatrix.isEmpty() && Library.numTaskFinished != Library.numAllTask) {
			// poll out the first event, and process according to event type
			msg = SimMatrix.pollFirst();
			SimMatrix.setSimuTime(msg.occurTime);
			if (msg.type.equals("logging"))
				Library.procLoggingEvent();
			else if (msg.type.equals("kvs"))
				sm.schedulers[msg.destId].procKVSEvent(msg);
			else if (msg.type.equals("kvs return"))
				sm.schedulers[msg.destId].procKVSRetEvent(msg);
			else if (msg.type.equals("work steal"))
				sm.schedulers[msg.destId].procWorkStealEvent(msg, sm.schedulers);
			else if (msg.type.equals("request task"))
				sm.schedulers[msg.destId].procReqTaskEvent(msg);
			else if (msg.type.equals("send task"))
				sm.schedulers[msg.destId].procRetReqTaskEvent(msg);
			else if (msg.type.equals("request data"))
				sm.schedulers[msg.destId].procReqDataEvent(msg);
			else if (msg.type.equals("return req data"))
				sm.schedulers[msg.destId].procRetReqDataEvent(msg);
			else if (msg.type.equals("push task"))
				sm.schedulers[msg.destId].procPushTaskEvent(msg);
			else 
				System.out.println("unknown event type!");
		}
		// Stop screen capture, create video from captures
		// rec.record = false;
		// System.out.println("Screen Capture Stopped");
		/*
		 * try { rec.makeVideo(System.currentTimeMillis() + ".mov"); } catch
		 * (Exception e) { e.printStackTrace(); }
		 */
		try {
			Library.summaryLogBW.flush();
			Library.summaryLogBW.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		sm.outputResult(start, end);
		// TODO code application logic here
	}
}