import tensorflow as tf
import os

tf.app.flags.DEFINE_integer("task_index", 0, "Index of the worker task")
FLAGS = tf.app.flags.FLAGS

num_features = 33762578
eta = 0.01
num_iterations = 1000

g = tf.Graph()

with g.as_default():

    # creating a model variable on task 0. This is a process running on node vm-48-1
    with tf.device("/job:worker/task:0"):
        w = tf.Variable(tf.ones([num_features,]), name="model")

    # creating only reader and gradient computation operator
    # here, they emit predefined tensors. however, they can be defined as reader
    # operators as done in "exampleReadCriteoData.py"
    with tf.device("/job:worker/task:%d" % FLAGS.task_index):
        filename_queue = tf.train.string_input_producer(["/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr-tiny/tfrecords00"], num_epochs=None)
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
        value = features['value']

        dense_feature = tf.sparse_to_dense(tf.sparse_tensor_to_dense(index),
                                            [num_features,],
        #                               tf.constant([33762578, 1], dtype=tf.int64),
                                            tf.sparse_tensor_to_dense(value))
	
        dot = tf.reduce_sum(tf.mul(w,tf.transpose(dense_feature)))
        dot = tf.Print(dot, [dot], "dot on %d"%FLAGS.task_index)
        local_gradient = tf.mul(tf.mul(label, tf.sigmoid(tf.mul(label,dot)-1)),dense_feature)
        
    with tf.device("/job:worker/task:0"):
        assign_op = w.assign_sub(tf.mul(local_gradient, eta))


    with tf.Session("grpc://vm-23-%d:2222" % (FLAGS.task_index+1)) as sess:
	    cord = tf.train.Coordinator()
        threads = tf.train.start_queue_runners(sess=sess,  coord=cord)
        # only one client initializes the variable
        if FLAGS.task_index == 0:
            sess.run(tf.initialize_all_variables())
        for i in range(0, num_iterations):
            sess.run(assign_op)
            print w.eval()
	    cord.request_stop()
	    cord.join(threads)
        sess.close()