import os
import psutil
import time
import datetime
import signal
import sys

#pid = os.getpid()
PROCESS_NAME = "python"
#PROG_NAME = "cifar10_train.py"
PROG_NAME = "resource_monitor.py"
SLEEP_DURATION=5
ETH_NAME='eno1d1'

logfile = open('resource_usage.log', 'a')
def signal_handler(signal, frame):
            print('Exiting')
            logfile.close()
            sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

prev_cpu_time = 0
prev_net_sent = psutil.net_io_counters(pernic=True)[ETH_NAME].bytes_sent
prev_net_recv = psutil.net_io_counters(pernic=True)[ETH_NAME].bytes_recv

while True:
    time.sleep(1)
    if int(time.time())%SLEEP_DURATION is not 0:
        continue
    print time.time(), int(time.time())
    pid = os.getpid()
    for proc in psutil.process_iter():
        if proc.name() == PROCESS_NAME: #and proc.cmdline()[-1].endswith(PROG_NAME):
            #l = [str(time.time()), str(datetime.datetime.now()), str(proc), str(proc.cmdline()), str(proc.cpu_times()), str(proc.memory_info()), str(psutil.net_io_counters(pernic=True)[ETH_NAME])]

            l = [str(time.time()), str(datetime.datetime.now()), str(proc), str(proc.cpu_times().user + proc.cpu_times().system - prev_cpu_time), str(proc.memory_info().rss), str(proc.memory_info().vms), str(psutil.net_io_counters(pernic=True)[ETH_NAME].bytes_sent - prev_net_sent), str(psutil.net_io_counters(pernic=True)[ETH_NAME].bytes_recv - prev_net_recv)]
            prev_cpu_time = proc.cpu_times().user + proc.cpu_times().system
            prev_net_sent = psutil.net_io_counters(pernic=True)[ETH_NAME].bytes_sent
            prev_net_recv = psutil.net_io_counters(pernic=True)[ETH_NAME].bytes_recv
            line = ' '.join(l)
            logfile.write("%s\n" % str(line))
            logfile.flush()
