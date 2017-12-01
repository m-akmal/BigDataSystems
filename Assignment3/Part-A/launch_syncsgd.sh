#!/bin/bash
source tfdefs.sh
start_cluster startserver.py
# start multiple clients
echo "starting sync sgd"
python synchronoussgd.py 
sleep 2 # wait for variable to be initialized
