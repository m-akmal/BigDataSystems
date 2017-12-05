import os
import psutil
import time
import datetime
import signal
import sys

#pid = os.getpid()
PROCESS_NAME = "mlr_main"
#PROG_NAME = "cifar10_train.py"
PROG_NAME = "test.py"
SLEEP_DURATION=2

logfile = open('resource_usage.log', 'a')
def signal_handler(signal, frame):
            print('Exiting')
            logfile.close()
            sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

while True:
    time.sleep(1)
    if int(time.time())%SLEEP_DURATION is not 0:
        continue
    print time.time(), int(time.time())
    pid = os.getpid()
    for proc in psutil.process_iter():
        if proc.name() == PROCESS_NAME: #and proc.cmdline()[-1].endswith(PROG_NAME):
            l = [str(time.time()), str(datetime.datetime.now()), str(proc), str(proc.cmdline()), str(proc.cpu_times()), str(proc.memory_info())]
            line = ' '.join(l)
            logfile.write("%s\n" % str(line))
            logfile.flush()

