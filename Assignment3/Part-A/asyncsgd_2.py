import tensorflow as tf
import os

tf.app.flags.DEFINE_integer("task_index", 0, "Index of the worker task")
FLAGS = tf.app.flags.FLAGS

num_features = 33762578
eta = 0.01
num_iterations = 20000000
check_error_iteration = 5

files = {
    0: ['/home/pavan/bigdata/Assignment3/data/criteo-tfr-tiny/tfrecords01']
    # 1: ['/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords05', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords06', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords07', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords08', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords09'],
    # 2: ['/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords10', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords11', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords12', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords13', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords14'],
    # 3: ['/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords15', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords16', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords17', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords18', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords19'],
    # 4: ['/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords20', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords21', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords22']
}
test_file = '/home/pavan/bigdata/Assignment3/data/criteo-tfr-tiny/tfrecords01'

def sign(dot):
    if dot<=0:
        return -1
    else:
        return 1

def calculate_avg_error(iteration):
    with tf.Session("grpc://localhost:2222" % 1) as test_sess:
        print("calculating error for %d" % iteration)
        error_count = 0
        total_count = 0
        record_iterator = tf.python_io.tf_record_iterator(path=test_file)
        for string_record in record_iterator:
            total_count = total_count+1
            features = tf.parse_single_example(string_record,
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
                                                tf.sparse_tensor_to_dense(value))

            # -- SGD error computation
            dot = tf.reduce_sum(tf.multiply(w, tf.transpose(dense_feature)))
            error = 1 if (label != sign(dot.eval())) else 0
            if error == 1:
                error_count=error_count+1
        error_count = tf.Print(error_count, [error_count], "error count out of %d for iteration %d = "%(total_count,iteration))
        test_sess.run(error_count)


g = tf.Graph()

with g.as_default():

    # creating a model variable on task 0. This is a process running on node vm-23-1
    with tf.device("/job:worker/task:0"):
        w = tf.Variable(tf.ones([num_features,]), name="model")
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
        value = features['value']

        dense_feature = tf.sparse_to_dense(tf.sparse_tensor_to_dense(index),
                                            [num_features,],
                                            tf.sparse_tensor_to_dense(value))
	
        dot = tf.reduce_sum(tf.multiply(w,tf.transpose(dense_feature)))
        dot = tf.Print(dot, [dot], "dot on %d"%FLAGS.task_index)
        local_gradient = tf.multiply(tf.multiply(label, tf.sigmoid(tf.multiply(label,dot)-1)),dense_feature)
        
    with tf.device("/job:worker/task:0"):
        assign_op = w.assign_sub(tf.multiply(local_gradient, eta))
        increment_op = count.assign_add(1)


    with tf.Session("grpc://localhost:2222" % (FLAGS.task_index+1)) as sess:
        cord = tf.train.Coordinator()
        threads = tf.train.start_queue_runners(sess=sess, coord=cord)
        if FLAGS.task_index == 0:
            sess.run(tf.initialize_all_variables())
        for i in range(0, num_iterations):
            sess.run(assign_op)
            sess.run(increment_op)
            if count.value%check_error_iteration==0:
                calculate_avg_error(i)
            # print w.eval()
	    cord.request_stop()
	    cord.join(threads)
        sess.close()