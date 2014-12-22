dagType=$1
dagPara=$2

i=100
#f=0
#t=0

while [ $i -le 100 ]
do
#	if [ $i -eq 128 ]
#	then
#		if [ $t -eq 0 ]
#		then
#			f=1
#			i=100
#		fi
#	fi
	
	echo -e "NumComputeNode\t$i\nNumCorePerNode\t2\nNumTaskPerCore\t1000\nMaxTaskLength\t0.1\nDagType\t$dagType\nDagPara\t$dagPara\nNetworkBandwidth\t10000000000.0\nNetworkLatency\t0.0006\nPackOverhead\t0.0006\nUnPackOverhead\t0.0006\nSingleMsgSize\t100\nProcTimePerTask\t0.001\nProcTimePerKVSRequest\t0.001\nTaskLog\ttrue\nRatioThreshold\t5000000000.0\nInitPollInterval\t0.001\nPollIntervalUB\t50\nLogTimeInterval\t1.0\nVisualTimeInterval\t0.5\nLocalQueueTimeThreshold\t10.0\nScreenCapMilInterval\t50" > config.$i
	
	j=1
	while [ $j -le 6 ] 
	do
		java -Xms7500m -Xmx7500m SimMatrix ./config.$i >> summary.$i.$j.$dagType
		j=$(($j+1))
	done
	
	#if [ $f -eq 1 ] 
	#then
	#	f=0
	#	i=64
	#	t=1
	#fi
	i=$(($i*2))
done

