SimMatrix
=====================================================================================================
This is the SimMatrix software with source code.

Developer: Ke Wang
Email: kwang22@hawk.iit.edu
Homepage: http://datasys.cs.iit.edu/~kewang/

All the copy right reserved
=====================================================================================================




=====================================================================================================
To compile it, go to the src directory:

execute: javac *.java
=====================================================================================================





=====================================================================================================
To run it, if you want to run the centralized simulator:

execute: java CentralSimulator $numNodes $numCoresPerNode $numTaskPerCore $maxTaskLength

if you want to run the distributed simulator:

execute: java DistributedSimulator $numNodes $numCoresPerNode $numTaskPerCore $maxTaskLength


There are four arguments:
$numNodes specifies how many compute nodes
$numCoresPerNode specifies how many cores a compute node has
$numTaskPerCore specifies how many tasks per core
$maxTaskLength specifies the maximum task length, in the program, task lengths are generated with uniform random distribution ranging from [0, maxTaskLength)

Of course, you can add a file reader to read the task lengths from a file; To change the task length, you can change the "doDispatch" method of the "Dipatcher.java" file, and the "execute" method of the "ComputeNodeD.java" class
=====================================================================================================
