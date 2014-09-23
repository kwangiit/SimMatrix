import java.util.*;

public class Task {
	int taskId;
	TaskMetaData taskMD;
	int numParentDataRecv;
	String data;
	int numChildMDUpdated;
}


class TaskMetaData {
	int taskId;
	int indegree;
	ArrayList<Integer> parent;
	ArrayList<Integer> children;
	ArrayList<String> dataNameList;
	ArrayList<Integer> dataSize;
	int allDataSize;
	
	int compareTaskMetaData(TaskMetaData destTaskMD) {
		if ((this == null && destTaskMD != null) || (this != null && destTaskMD == null))
			return 1;
		if (this == null && destTaskMD == null)
			return 0;
		
		if (taskId != destTaskMD.taskId)
			return 1;
		
		if (indegree != destTaskMD.indegree)
			return 1;
		
		if ((parent == null && destTaskMD.parent != null) 
				|| (parent != null && destTaskMD.parent == null))
			return 1;
		if (parent != null && destTaskMD.parent != null) {
			if (parent.size() != destTaskMD.parent.size())
				return 1;
			for (int i = 0; i < parent.size(); i++) {
				if (!(parent.get(i).equals(destTaskMD.parent.get(i))))
					return 1;
			}
		}
		
		if ((children == null && destTaskMD.children != null) 
				|| (children != null && destTaskMD.children == null))
			return 1;
		if (children != null && destTaskMD.children != null) {
			if (children.size() != destTaskMD.children.size())
				return 1;
			for (int i = 0; i < children.size(); i++) {
				if (!(children.get(i).equals(destTaskMD.children.get(i))))
					return 1;
			}
		}
		
		if ((dataNameList == null && destTaskMD.dataNameList != null) 
				|| (dataNameList != null && destTaskMD.dataNameList == null))
			return 1;
		if (dataNameList != null && destTaskMD.dataNameList != null) {
			if (dataNameList.size() != destTaskMD.dataNameList.size())
				return 1;
			for (int i = 0; i < dataNameList.size(); i++) {
				if (!(dataNameList.get(i).equals(destTaskMD.dataNameList.get(i))))
					return 1;
			}
		}
		
		if ((dataSize == null && destTaskMD.dataSize != null) 
				|| (dataSize != null && destTaskMD.dataSize == null))
			return 1;
		if (dataSize != null && destTaskMD.dataSize != null) {
			if (dataSize.size() != destTaskMD.dataSize.size())
				return 1;
			for (int i = 0; i < dataSize.size(); i++) {
				if (!(dataSize.get(i).equals(destTaskMD.dataSize.get(i))))
					return 1;
			}
		}
		
		if (allDataSize != destTaskMD.allDataSize)
			return 1;
		
		return 0;
	}
	
	TaskMetaData copyTaskMetaData() {
		TaskMetaData taskMD = new TaskMetaData();
		taskMD.taskId = taskId;
		taskMD.indegree = indegree;
		taskMD.parent = new ArrayList<Integer>();
		for (int i = 0; i < parent.size(); i++) {
			taskMD.parent.add(parent.get(i).intValue());
		}
		taskMD.children = new ArrayList<Integer>();
		for (int i = 0; i < children.size(); i++) {
			taskMD.children.add(children.get(i).intValue());
		}
		taskMD.dataNameList = new ArrayList<String>();
		for (int i = 0; i < dataNameList.size(); i++) {
			taskMD.dataNameList.add(dataNameList.get(i).substring(0));
		}
		taskMD.dataSize = new ArrayList<Integer>();
		for (int i = 0; i < dataSize.size(); i++) {
			taskMD.dataSize.add(dataSize.get(i).intValue());
		}
		taskMD.allDataSize = allDataSize;
		return taskMD;
	}
}


class TaskDesc {
	int taskId;
	String users;
	String dir;
	String cmd;
	int dataLength;
}