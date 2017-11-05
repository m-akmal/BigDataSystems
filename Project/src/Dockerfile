FROM 6fb674952285 

# RUN apt-get update
# RUN apt-get -y install python-pip python-dev python-virtualenv 
# RUN virtualenv --system-site-packages tensorflow
RUN /bin/bash -c "source ./tensorflow/bin/activate"
# RUN easy_install -U pip
# RUN pip install --upgrade tensorflow

# RUN apt-get -y install git-all
# RUN git clone https://github.com/tensorflow/models.git

CMD python ./models/tutorials/image/cifar10/cifar10_train.py;tensorboard --logdir=/tmp/cifar10_train &

