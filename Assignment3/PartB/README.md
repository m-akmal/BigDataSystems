# Group 23 Part B GraphX

build.sbt : File used by SBT Tool to create Jar of compiled Scala Apps.

## APPLICATION 1

Program: PartBApplication1Question1.scala
    Arguments: <hdfs_file_path> <num_of_iterations> <num_of_partitions>
    Defaults: "./soc-LiveJournal1.txt 20            50

Command to run the program:
./PartBApplication1Question1.sh

Command to submit spark job
    ```$SPARK_HOME/bin/spark-submit --class PartBApplication1Question1 --master spark://10.254.0.146:7077 \
                                 file:///home/ubuntu/rohit/assignment3/PartB/target/scala-2.11/page-rank-graphx-group-23_2.11-1.0.jar \
	                            /graphx/soc-LiveJournal1.txt \
                                 20```

Program Logic:
Graph Construction:
Code reads fromNodeId and toNodeId from source file into datafile variable.
The string line is split into to tuple of srcNodeId and destNodeID

graph - Graph of vertices(unique list of fromNodeid) and edges(list of tuples of node pairs)

outDegrees - calculates out degree for each vertex

runGraph - Joins vertices with their out degrees.

Loop for 20 iterations:
Calculate rank to be transferred to neighbours for each vertex.
Send the calculated amount to each neighbour via sendToDst by looping on each edgeTriplet
Reduce all received rank_updates by summing them and updating current rank for each vertex.


rankGraph.triplets.count actions all the calculations.

## APPLICATION 2

### PART 1 : Find the number of edges where the number of words in the source vertex is strictly larger than the number of words in the destination vertex.

#### COMMAND TO RUN : `./PartBApplication2Question1.sh`

#### COMMAND TO SUBMIT SPARK JOB : `$SPARK_HOME/bin/spark-submit --class PartBApp2WordCompare --master spark://10.254.0.146:7077 file:///home/ubuntu/a3pB/target/scala-2.11/build-graph-group-23_2.11-1.0.jar vertex_info.txt`

#### PROGRAM LOGIC :

1. Read input file line by line, ignore 1st word which is the timestamp.
2. Treat each line as a separte vertex.
3. Find intersecting words for every pair of vertices.
4. Set Edge property as 1, if source vertex had more words.
5. Set Edge property to 2 in other cases.
6. Create graph using above vertices and edges.
7. Filter all the vertice that have edge value as 1 and count.

### PART 2 : Find the most popular vertex. A vertex is the most popular if it has the most number of edges to its neighbors and it has the maximum number of words. If there are many satisfying the above criteria, pick any one you desire.

#### COMMAND TO RUN : `./PartBApplication2Question2.sh`

#### COMMAND TO SUBMIT SPARK JOB : `$SPARK_HOME/bin/spark-submit --class PartBApp2MostPopular --master spark://10.254.0.146:7077 file:///home/ubuntu/a3pB/target/scala-2.11/build-graph-group-23_2.11-1.0.jar vertex_info.txt`

#### PROGRAM LOGIC :

1. Read input file line by line, ignore 1st word which is the timestamp.
2. Treat each line as a separte vertex.
3. Find intersecting words for every pair of vertices.
4. Set Edge property as 1, if source vertex had more words.
5. Set Edge property to 2 in other cases.
6. Create graph using above vertices and edges.
7. Aggregate neighbouring vertices by adding all the count recieved at a particular vertex. Create VertexRDD.
8. Define max function such that it returns the vertex with highest count or highest number of words in case of a tie.
9. Reduce the VertexRDD by applying the max function to get the most popular vertex.

### PART 3 : Find the number of edges where the number of words in the source vertex is strictly larger than the number of words in the destination vertex.

#### COMMAND TO RUN : `./PartBApplication2Question3.sh`

#### COMMAND TO SUBMIT SPARK JOB : `PARK_HOME/bin/spark-submit --class PartBApp2AverageCount --master spark://10.254.0.146:7077 file:///home/ubuntu/a3pB/target/scala-2.11/build-graph-group-23_2.11-1.0.jar vertex_info.txt`

#### PROGRAM LOGIC :

1. Read input file line by line, ignore 1st word which is the timestamp.
2. Treat each line as a separte vertex.
3. Find intersecting words for every pair of vertices.
4. Set Edge property as 1, if source vertex had more words.
5. Set Edge property to 2 in other cases.
6. Create graph using above vertices and edges.
7. Aggregate neighbouring vertices by adding all the counts and destination lengths recieved at a particular vertex. Create VertexRDD.
8. Define average function such that it returns the ratio of destination lengths to the counts for each vertex.
9. Map the Vertex RDD by applying average value function on each vertex. Crates poplarVertexRDD.
10. Map the poplarVertexRDD by associating a string contating (vertex_number, average_words) information to each vertex. Creates popularVertexPrintRDD.
11. Print each record in popularVertexPrintRDD.

