import os
import tensorflow as tf


num_features = 33762578
num_iterations = 20000000
eta = 0.01
check_error_iteration = 1000


# files = {
#     0: ['/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords00', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords01', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords02', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords03', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords04'],
#     1: ['/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords05', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords06', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords07', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords08', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords09'],
#     2: ['/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords10', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords11', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords12', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords13', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords14'],
#     3: ['/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords15', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords16', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords17', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords18', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords19'],
#     4: ['/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords20', '/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords21']
# }

# test_file = ['/home/ubuntu/rohit/tf/syncsgd/data/criteo-tfr/tfrecords22']

files = {
    0: ['/home/pavan/bigdata/Assignment3/data/criteo-tfr-tiny/tfrecords00','/home/pavan/bigdata/Assignment3/data/criteo-tfr-tiny/tfrecords01','/home/pavan/bigdata/Assignment3/data/criteo-tfr-tiny/tfrecords02']
    ,1: ['/home/pavan/bigdata/Assignment3/data/criteo-tfr-tiny/tfrecords03','/home/pavan/bigdata/Assignment3/data/criteo-tfr-tiny/tfrecords04','/home/pavan/bigdata/Assignment3/data/criteo-tfr-tiny/tfrecords05']
    ,2: ['/home/pavan/bigdata/Assignment3/data/criteo-tfr-tiny/tfrecords06','/home/pavan/bigdata/Assignment3/data/criteo-tfr-tiny/tfrecords07','/home/pavan/bigdata/Assignment3/data/criteo-tfr-tiny/tfrecords08']

}

test_file = ['/home/pavan/bigdata/Assignment3/data/criteo-tfr-tiny/tfrecords10']
batch_size = 10
min_after_dequeue = batch_size

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

        test_dot = tf.reduce_sum(tf.mul(w, tf.transpose(test_feature)))
        return tf.equal(test_label, tf.sign(test_dot))

def read_file(filename_queue):    
    record_reader = tf.TFRecordReader()
    _, record_string = record_reader.read(filename_queue)
    record_features = tf.parse_single_example(record_string,
                                       features={
                                        'label': tf.FixedLenFeature([1], dtype=tf.int64),
                                        'index' : tf.VarLenFeature(dtype=tf.int64),
                                        'value' : tf.VarLenFeature(dtype=tf.float32),
                                       }
                                      )
    label = record_features['label']
    # casting so we can multiply with dot product
    label = tf.cast(label, tf.float32)
    index = record_features['index']
    value = record_features['value']

    dense_feature = tf.sparse_to_dense(tf.sparse_tensor_to_dense(index),
                                        [num_features,],
                                        tf.sparse_tensor_to_dense(value))
    return dense_feature, label

def input_pipeline(file_list):
    filename_queue = tf.train.string_input_producer(file_list, num_epochs=None)
    features, label = read_file(filename_queue)
    example_batch, label_batch = tf.train.shuffle_batch(
      [features, label], batch_size=batch_size, capacity=4*batch_size,
      min_after_dequeue=min_after_dequeue)
    return example_batch, label_batch

def local_computation(file_list):
    
    features_batch, label_batch = input_pipeline(file_list)
    
    for example in range(0, batch_size):
        dot = tf.reduce_sum(tf.mul(w, tf.transpose(features_batch[example])))
        local_gradient = label_batch[example] * (tf.sigmoid(label_batch[example] * dot) - 1) * features_batch[example]
        gradients.append(local_gradient)

g = tf.Graph()

with g.as_default():

    error_count = 0
    # creating a model variable on task 0. This is a process running on node vm-48-1
    with tf.device("/job:worker/task:0"):
        w = tf.Variable(tf.ones([num_features,]), name="model")

    # creating 5 reader operators to be placed on different operators
    # here, they emit predefined tensors. however, they can be defined as reader
    # operators as done in "exampleReadCriteoData.py"
    gradients = []
    for i in range(0, 5):
        with tf.device("/job:worker/task:%d" % i):
            local_computation(files[i])

    # we create an operator to aggregate the local gradients
    with tf.device("/job:worker/task:0"):
        aggregator = tf.add_n(gradients)
        assign_op = w.assign_sub(aggregator * eta)
        assign_op = tf.Print(assign_op, [assign_op], "new value of w")
        error_op = calculate_error()
	# assign_op = tf.Print(assign_op,[assign_op], "assign op")
        # assign_op = w.assign_add(aggregator)

    with tf.Session("grpc://vm-23-1:2222") as sess:
	print "starting queue runners"
        tf.train.start_queue_runners(sess=sess)
        sess.run(tf.initialize_all_variables())
        for i in range(1, num_iterations+1):
            sess.run(assign_op)
            # print w.eval() 				
            if i % check_error_iteration == 0:
                for err in range(0, 10):
	            # sess.run(avg_error_op)
                    error_count = error_count+ (0 if error_op.eval() == True else 1)
                print "total error count at %d (out of 10) = %d " % (i, error_count)
                msg = tf.Print(error_count, [error_count], "error count from tensor at iter %d" % i)
                msg.eval()
                error_count = 0
                
        sess.close()


