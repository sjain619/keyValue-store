Programming language used: Java

To compile the code on remote.cs.binghamton.edu computers execute the following commands:


## To generate cassandra.java using protoc :
    $> bash
    $> export PATH=/home/vchaska1/protobuf/bin:$PATH

### cd to Consistent-Key-Value-Store the run command belows :

    $> protoc --java_out=./src/ ./src/cassandra.proto


## Steps to Compile and Run the program

### 1. Compile the code using Makefile which is already present in the repo.

    $> make

### 2. Run the multiple coordinator/replica servers by providing below command line arguments :
    
    $>./coordinator.sh <Nodes.txt> <port> <readRepairOrHintedHandOff>

    readRepairOrHintedHandOff = 1 => readRepair Mode
    readRepairOrHintedHandOff = 2 => hintedHandOff Mode

Prepare the node file containing the Names, IPs and ports of the replicas.

Run the coordinator on the server whose IP is present in nodes file. Use different ports on each remote01, remote02 ....

>$ ./coordinator.sh <Nodes.txt> <port>

Run the client on a different remote# than coordinator:

>$ ./client.sh <Nodes.txt>

A console will be presented next:

a) For Read - replicaname, read, key, consistency-level
b) For Write - replicaname, write, key, value, consistency-level

The key if found in read will be returned and a new key will be written in write mode.





