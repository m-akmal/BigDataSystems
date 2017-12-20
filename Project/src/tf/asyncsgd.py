import tensorflow as tf
import os

tf.app.flags.DEFINE_integer("task_index", 0, "Index of the worker task")
FLAGS = tf.app.flags.FLAGS

num_features = 33762578
eta = 0.5
num_iterations = 20000000
check_error_iteration = 2   
gradient_limit=0.01

files = {
    0: ['/home/pavan/bigdata/Assignment3/data/criteo-tfr-tiny/tfrecords00','/home/pavan/bigdata/Assignment3/data/criteo-tfr-tiny/tfrecords01','/home/pavan/bigdata/Assignment3/data/criteo-tfr-tiny/tfrecords02']
    ,1: ['/home/pavan/bigdata/Assignment3/data/criteo-tfr-tiny/tfrecords03','/home/pavan/bigdata/Assignment3/data/criteo-tfr-tiny/tfrecords04','/home/pavan/bigdata/Assignment3/data/criteo-tfr-tiny/tfrecords05']
    ,2: ['/home/pavan/bigdata/Assignment3/data/criteo-tfr-tiny/tfrecords06','/home/pavan/bigdata/Assignment3/data/criteo-tfr-tiny/tfrecords07','/home/pavan/bigdata/Assignment3/data/criteo-tfr-tiny/tfrecords08']
}

test_file = ['/home/pavan/bigdata/Assignment3/data/criteo-tfr-tiny/tfrecords10']

def calculate_error():
    with tf.device("/job:worker/task:0"):
        test_filename_queue = tf.train.string_input_producer(test_file, num_epochs=None)
        test_reader = tf.TFRecordReader()

        _, serialized_example = test_reader.read(test_filename_queue)

        test_features = tf.parse_single_example(serialized_example,
                                        features={
                                            'label': tf.FixedLenFeature([1], dtype=tf.int64),
                                            'index' : tf.VarLenFeature(dtype=tf.int64),
                                            'value' : tf.VarLenFeature(dtype=tf.float32),
                                        }
                                        )

        test_label = test_features['label']
        # casting so we can multiply with dot product
        test_label = tf.cast(test_label, tf.float32)
        test_index = test_features['index']
        test_value = test_features['value']

        test_feature = tf.sparse_to_dense(tf.sparse_tensor_to_dense(test_index),
                                            [num_features,],
                                            tf.sparse_tensor_to_dense(test_value))
	
        test_dot = tf.reduce_sum(tf.multiply(w,tf.transpose(test_feature)))
        test_dot = tf.Print(test_dot, [test_dot], "test dot")
        test_label = tf.Print(test_label, [test_label],"test label")
        return tf.abs(test_label - test_dot)
    	# error = 1 if (label != tf.sign(dot)) else 0
        # error = tf.Print(error, [error], "error value")
        # return error

g = tf.Graph()

with g.as_default():

    error_count = 0 
    # count = 0
    # creating a model variable on task 0. This is a process running on node vm-23-1
    with tf.device("/job:worker/task:0"):
        ones = tf.Variable(tf.ones([num_features,]), name="model") 
        zeros = tf.Variable(tf.zeros([num_features,]), name="model") 
        seq_mask = tf.sequence_mask([num_features,])
        w = tf.Variable(tf.zeros([num_features,]), name="model")
        count = tf.Variable(1, "count")

    # creating only reader and gradient computation operator
    # here, they emit predefined tensors. however, they can be defined as reader
    # operators as done in "exampleReadCriteoData.py"
    with tf.device("/job:worker/task:%d" % FLAGS.task_index):
        filename_queue = tf.train.string_input_producer(files[FLAGS.task_index], num_epochs=None)
        reader = tf.TFRecordReader()

        _, serialized_example = reader.read(filename_queue)

        features = tf.parse_single_example(serialized_example,
                                        features={
                                            'label': tf.FixedLenFeature([1], dtype=tf.int64),
                                            'index' : tf.VarLenFeature(dtype=tf.int64),
                                            'value' : tf.VarLenFeature(dtype=tf.float32),
                                        }
                                        )

        label = features['label']
        # casting so we can multiply with dot product
        label = tf.cast(label, tf.float32)
        index = features['index']
        sparse_features = features['value']

        # dense_feature = tf.sparse_to_dense(tf.sparse_tensor_to_dense(index),
        #                                     [num_features,],
        #                                     tf.sparse_tensor_to_dense(sparse_features))
	
        
        # sparse
        gathered_w = tf.gather(w, tf.sparse_tensor_to_dense(index))   
        gathered_features = sparse_features.values
        
        dot = tf.reduce_sum(tf.multiply(gathered_w, tf.transpose(gathered_features)))
        # dots.append(dot)
        local_gradient = label * (tf.sigmoid(label * dot) - 1) * gathered_features
        # print("local grad::: ", local_gradient)
        local_gradient = tf.Print(local_gradient, [local_gradient], "local gradient")
        # -- sparse
 
    with tf.device("/job:worker/task:0"):
        print "updating value of w"
        # Sparse value
        assign_op = w.assign_sub((tf.sparse_to_dense(sparse_indices=tf.sparse_tensor_to_dense(index), output_shape=[num_features,], sparse_values=tf.reshape(local_gradient, [-1])))*eta)
        # -- sparse
        assign_op = tf.Print(assign_op, [assign_op], "new value of w")
        increment_op = count.assign_add(1)
        error_op = calculate_error()

    with tf.Session("grpc://localhost:2220") as sess:
        # count_lines()
#        cord = tf.train.Coordinator()
        threads = tf.train.start_queue_runners(sess=sess)
        if FLAGS.task_index == 0:
	    #print("init variables")
            sess.run(tf.initialize_all_variables())
        for i in range(0, num_iterations):
	    #print "attempting assign_op"
            sess.run(assign_op)
            sess.run(increment_op)
            print("processed:", count.eval())
	    if count.eval()%check_error_iteration == 0:
                for err in range(0,10):
	            # sess.run(avg_error_op)
                    error_count = error_count+ error_op.eval()
                print("total error value at %d (out of 10) = %d " % (count.eval(), error_count/10))
                msg = tf.Print(error_count,[error_count],"error count from tensor at iter %d"%count.eval())
                msg.eval()
                error_count = 0
            #    print("calculating error")
            #    calculate_avg_error(i)
            #print w.eval()
	    #cord.request_stop()
	    #cord.join(threads)
        sess.close()
