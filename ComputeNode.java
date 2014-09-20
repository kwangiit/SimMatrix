/**
 *
 * @author Ke Wang Since June 2011
 */

/*
 * this class is the compute node of the centralized simulator
 */

public class ComputeNode
{
    int nodeId;
    int numCores;
    int numIdleCores;

    public ComputeNode(int nodeId, int numCores, int numIdleCores)
    {
        this.nodeId = nodeId;
        this.numCores = numCores;
        this.numIdleCores = numIdleCores;
    }
}