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

public class DistributedSimulator {
	private static double simuTime; // simulation time
	private static TreeSet<Event> ts; // global event queue

	Client client; // a client
	Scheduler[] nodes; // all the compute nodes

	JFrame window; // the window to do visualization
	ModCanvas canvas; // the canvas for painting

	double throughput;

	/* static methods to operate the simulation time and event queue */
	public static void setSimuTime(double time) {
		DistributedSimulator.simuTime = time;
	}

	public static double getSimuTime() {
		return DistributedSimulator.simuTime;
	}

	public static boolean isEmpty() {
		return DistributedSimulator.ts.isEmpty();
	}

	public static void add(Event event) {
		DistributedSimulator.ts.add(event);
	}

	public static Event pollFirst() {
		return DistributedSimulator.ts.pollFirst();
	}

	/* initialize the parameters from the library */
	public void initLibrary(String[] args) {
		Library.numComputeNode = Integer.parseInt(args[0]);
		Library.nodeId = new int[Library.numComputeNode];
		for (int i = 0; i < Library.numComputeNode; i++)
			Library.nodeId[i] = i;
		Library.numCorePerNode = Integer.parseInt(args[1]);
		Library.numTaskPerCore = Integer.parseInt(args[2]);
		Library.maxTaskLength = Double.parseDouble(args[3]);
		Library.dagType = args[4];
		Library.dagPara = Integer.parseInt(args[5]);
		
		Library.linkSpeed = 6800000000.0; // b/sec
		Library.netLat = 0.0001; // second
		Library.oneMsgSize = 1024; // Bytes
		Library.packOverhead = Library.unpackOverhead = 0.000005;

		Library.oneMsgCommTime = Library.getCommOverhead(Library.oneMsgSize);
		Library.procTimePerTask = 0.001;
		Library.procTimePerKVSRequest = 0.001;

		Library.numTaskToSubmit = 1000000;
		Library.numTaskLowBd = 200000;
		Library.numAllTask = Library.numComputeNode * 
				Library.numCorePerNode * Library.numTaskPerCore;
		Library.taskLog = false;
		Library.eventId = 0;
		if (Library.numComputeNode == 1) {
			Library.numNeigh = 0;
		} else {
			Library.numNeigh = (int) (Math.sqrt(Library.numComputeNode));
		}

		Library.infoMsgSize = 100; // Bytes
		Library.stealMsgCommTime = (double) Library.infoMsgSize * 8
				/ (double) Library.linkSpeed + Library.netLat;
		Library.numTaskSubmitted = 0;
		Library.numTaskFinished = 0;
		Library.numStealTask = 0;
		Library.logTimeInterval = 1.0;
		Library.visualTimeInterval = 0.5;

		// equivalent to 20 frames per second
		Library.screenCapMilInterval = 50;

		Library.numBusyCore = 0;
		Library.waitQueueLength = 0;

		try {
			Library.logBuffWriter = new BufferedWriter(new FileWriter(
					"summaryD_" + Library.numComputeNode + ".txt"));
		} catch (IOException e) {
			e.printStackTrace();
		}

		Library.runtime = Runtime.getRuntime();
		Library.numUser = "1";
		Library.numResource = "0";
		Library.numAllCore = (long) Library.numComputeNode
				* (long) Library.numCorePerNode;
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

		Library.target = new boolean[Library.numComputeNode];
		for (int i = 0; i < Library.numComputeNode; i++) {
			Library.target[i] = false;
		}

		/* counters */
		Library.numMsg = 0;
		Library.numWorkStealing = 0;
		Library.numFailWorkStealing = 0;
	}

	/* initialization of the simulation environment */
	public void initSimulation(String[] args) {
		System.out.println("Initializing...");
		initLibrary(args);
		DistributedSimulator.setSimuTime(0.0);
		DistributedSimulator.ts = new TreeSet<Event>();
		client = new Client(-1, Library.numAllTask);

		/* initialize nodes */
		nodes = new Scheduler[Library.numComputeNode];
		for (int i = 0; i < Library.numComputeNode; i++) {
			nodes[i] = new Scheduler(i, Library.numCorePerNode,
					Library.numNeigh);
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
	public void taskReceptionEventProcess(Event event) {
		Scheduler recvNode = nodes[event.destId];
		recvNode.readyTaskListSize += event.info;

		/* if there are idle cores, then execute tasks */
		if (recvNode.numIdleCore > 0) {
			recvNode.execute(DistributedSimulator.getSimuTime());
		}
		if (event.sourceId == -1) {
			Library.waitQueueLength += event.info;
			client.waitFlag = false;
		}
	}

	/* task end event processing */
	public void taskEndEventProcess(Event event) {
		Scheduler curNode = nodes[event.sourceId];
		curNode.numIdleCore++;
		curNode.numTaskFinished++;
		Library.numTaskFinished++;
		Library.numBusyCore--;
		if (curNode.id == 0 && !client.waitFlag) {
			/*
			 * if the number of waiting tasks of the first node is below the
			 * predefined threshold and the client still has tasks, then the
			 * client would send more tasks
			 */
			if (client.numLeftTasks > 0
					&& curNode.readyTaskListSize < Library.numTaskLowBd) {
				client.submitTaskToDispatcher(
						DistributedSimulator.getSimuTime()
								+ Library.oneMsgCommTime, 0, false);
			}
		}

		/*
		 * if current node still have more tasks, then execute tasks, otherwise
		 * do work tealing
		 */
		if (curNode.readyTaskListSize > 0) {
			curNode.execute(DistributedSimulator.getSimuTime());
		} else if (Library.numNeigh > 0) {
			// instead of enqueueing one event, directly call the routine
			curNode.askLoadInfo(nodes);
		}
	}

	/* do work stealing */
	public void stealEventProcess(Event event) {
		Scheduler curNode = nodes[event.sourceId];
		// will chain with request or other steal events
		curNode.askLoadInfo(nodes);
	}

	/* request task event processing */
	public void taskDispatchEventProcess(Event event) {
		Scheduler curNode = nodes[event.destId];

		/* send have of the load */
		int loadToSend = (int) Math
				.floor((curNode.readyTaskListSize - curNode.numIdleCore) / 2);

		double latency = 0;
		double msgSize = 0;

		/* if current node has more available tasks, then send tasks */
		if (curNode.readyTaskListSize >= loadToSend && loadToSend > 0) {
			int nodeToSend = event.sourceId;
			msgSize = loadToSend * Library.taskSize;
			latency = msgSize * 8 / Library.linkSpeed + Library.netLat;

			Event submission = new Event((byte) 0, loadToSend,
					DistributedSimulator.getSimuTime() + latency,
					curNode.id, nodeToSend, Library.eventId++);
			Library.numMsg++;
			DistributedSimulator.add(submission);
			curNode.readyTaskListSize -= loadToSend;
			curNode.numTaskDispatched += loadToSend;
		} else // otherwise, ask the neighbor to steal again
		{
			loadToSend = 0;
			// if it happens that the requested node didn't have any jobs
			latency = Library.stealMsgCommTime;
			Library.numMsg++;
			Library.numFailWorkStealing++;
			Event stealEvent = new Event((byte) 2, -1,
					DistributedSimulator.getSimuTime() + latency,
					event.sourceId, -2, Library.eventId++);
			DistributedSimulator.add(stealEvent);
		}
		if (curNode.id == 0 && !client.waitFlag) {
			if (client.numLeftTasks > 0
					&& curNode.readyTaskListSize < Library.numTaskLowBd) {
				client.submitTaskToDispatcher(
						DistributedSimulator.getSimuTime(), 0, false);
			}
		}
	}

	/* process the visualization event */
	public void visualEvent() {
		this.canvas.repaint();
		Event visEvent = new Event(
				(byte) 5,
				-1,
				DistributedSimulator.getSimuTime() + Library.visualTimeInterval,
				-2, -2, Library.eventId++);
		this.ts.add(visEvent);
	}

	/*
	 * calculate the coefficient variance of the number of tasks finished by
	 * each node
	 */
	public double coVari() {
		double sum = 0;
		double mean;
		for (int i = 0; i < nodes.length; i++) {
			sum += nodes[i].numTaskFinished;
		}
		mean = sum / nodes.length;
		sum = 0;
		for (int i = 0; i < nodes.length; i++) {
			sum += Math.pow(nodes[i].numTaskFinished - mean, 2);
		}
		double sd = Math.sqrt(sum / nodes.length);
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
				+ DistributedSimulator.getSimuTime());
		throughput = (double) (Library.numAllTask)
				/ DistributedSimulator.getSimuTime();
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
		if (args.length != 6) {
			System.out.println("Need three parameters: num_node, num_core_per_node, "
					+ "num_tasks_per_core, max_task_length, dag_type, data_para");
			System.exit(1);
		}
		long start = System.currentTimeMillis();
		DistributedSimulator ds = new DistributedSimulator();
		ds.initSimulation(args);
		// ds.initVisualization();
		ds.client.genDagAdjlist(Library.dagType, Library.dagPara);
		ds.client.genDagIndegree();
		ds.client.insertTaskMetaToKVS(ds.nodes);
		ds.client.splitTask(ds.nodes);
		//ds.client.submitTaskToDispatcher(DistributedSimulator.getSimuTime(), 0, false);
		
		// at the beginning, add a logging event
		Event logging = new Event((byte) 0, -1, null, 
				DistributedSimulator.getSimuTime(), -2, -2, Library.eventId++);
		DistributedSimulator.add(logging);
		
		/*
		 * first all the compute nodes check the waiting 
		 */
		for (int i = 0; i < Library.numComputeNode; i++) {
			ds.nodes[i].checkReadyTask();
		}
		
		for (int i = 0; i < Library.numComputeNode; i++) {
			DistributedSimulator.add(new Event((byte)3, -1, null, 
					DistributedSimulator.getSimuTime() + Math.random() * 
					0.001, i, -2, Library.eventId++));
			ds.nodes[i].ws = true;
		}
		/*
		 * DistributedSimulator.add(new Event((byte)5, -1,
		 * DistributedSimulator.getSimuTime(), -1, -1, Library.eventId++));
		 */
		Event event = null;
		while (!DistributedSimulator.isEmpty()
				&& Library.numTaskFinished != Library.numAllTask) {
			// poll out the first event, and process according to event type
			event = DistributedSimulator.pollFirst();
			DistributedSimulator.setSimuTime(event.occurTime);
			switch (event.type) {
			/*
			 * case 0: 
			 * case 1:
			 * case 2:
			 * case 3:
			 * case 4:
			 * case 5:
			 * case 6:
			 * case 7:
			 * case 8:
			 * case 9:
			 * case 10:
			 */
			case 0: // logging event
				Library.loggingEventProcess();
				break;
			case 1: // checking for ready task
				ds.nodes[event.destId].procCheckReadyTaskEvent(event);
				break;
			case 2: // responding for checking task metadata
				ds.taskEndEventProcess(event);
				break;
			case 3:
				ds.stealEventProcess(event);
				break;
			case 4:
				ds.taskDispatchEventProcess(event);
				break;
			case 5:
				ds.visualEvent();
				break;
			}
		}
		// Stop screen capture, create video from captures
		// rec.record = false;
		// System.out.println("Screen Capture Stopped");
		/*
		 * try { rec.makeVideo(System.currentTimeMillis() + ".mov"); } catch
		 * (Exception e) { e.printStackTrace(); }
		 */
		try {
			Library.logBuffWriter.flush();
			Library.logBuffWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		ds.outputResult(start, end);
		// TODO code application logic here
	}
}