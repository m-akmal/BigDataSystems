import tensorflow as tf
import os

#This prog is derived from exampleMatmulSingle.py

g = tf.Graph()

with g.as_default():
    tf.set_random_seed(1024)

    tf.logging.set_verbosity(tf.logging.DEBUG)

    N = 100000 # dimension of the matrix
    d = 100 # number of splits along one dimension. Thus, we will have 100 blocks
    M = int(N / d)

    def get_block_name(i, j):
        return "sub-matrix-"+str(i)+"-"+str(j)

    def get_intermediate_trace_name(i, j):
        return "inter-"+str(i)+"-"+str(j)

    #TRICK: Calculate machine for sub-matrix by ((i+j) % 5) 
    #This will place sub-matrtix (i,j) and (j,i) on same host
    #This placement strategy will reduce shuffle over network
    matrices = {}
    for i in range(0, d):
        for j in range(0, d):
            with tf.device("/job:worker/task:%d" % ( (i+j) % 5 )):
                matrix_name = get_block_name(i, j)
                matrices[matrix_name] = tf.random_uniform([M, M], name=matrix_name)

    #intermediate_traces will store sum of trace for all sub-matrices on each machine
    intermediate_traces = {0:0,1:0,2:0,3:0,4:0}
    import datetime
    print "Before matrix creation:",datetime.datetime.now()
    for i in range(0, d):
        for j in range(0, d):
            with tf.device("/job:worker/task:%d" % ( (i+j) % 5 )):
                A = matrices[get_block_name(i, j)]
                B = matrices[get_block_name(j, i)]
                traceForSubMatrix = tf.trace(tf.matmul(A, B))
                oldSum = intermediate_traces[(i+j) % 5]
                intermediate_traces[(i+j) % 5] = oldSum + traceForSubMatrix 
            print "After matrix creation:",datetime.datetime.now()

    with tf.device("/job:worker/task:0"):
        #Calculate total trace by summing up all elements from intermediate_traces
        retval = tf.add_n(intermediate_traces.values())
        print "After retval calculation:",datetime.datetime.now()

        config = tf.ConfigProto(log_device_placement=True)
        with tf.Session("grpc://vm-23-2:2222", config=config, graph=g) as sess:
            result = sess.run(retval)
            sess.close()
            print "Trace of the big matrix is = ", result
            print "SUCCESS"


