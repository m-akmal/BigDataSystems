#!/bin/sh

start_spark_job() {
    $SPARK_HOME/bin/spark-submit --class PartAQuestion3 --master spark://10.254.0.146:7077 \
                                 /home/ubuntu/rohit/assignment2/SparkStructuredStreaming/target/scala-2.11/sparkstreaming_2.11-1.0.jar \
	                         hdfs:///spark-streaming/stream hdfs:///spark-streaming/userlist.txt
}

echo "Clearing cache"
#pdsh -R ssh -w vm-23-[1-5] 'sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"'

io_read_command='iostat -d | tail -2 | head -1 | tr -s " " | cut -d " " -f5'
io_read_before=`pdsh -R ssh -w vm-23-[1-5] "$io_read_command" | cut -d ' ' -f2 | python -c "import sys; print(sum(int(l) for l in sys.stdin))"`

io_write_command='iostat -d | tail -2 | head -1 | tr -s " " | cut -d " " -f6'
io_write_before=`pdsh -R ssh -w vm-23-[1-5] "$io_write_command" | cut -d ' ' -f2 | python -c "import sys; print(sum(int(l) for l in sys.stdin))"`

#net_recv_command='netstat -i | grep eth0 | tr -s " " | cut -d " " -f4'
net_recv_command='cat /proc/net/dev | grep eth0 | tr -s " " | cut -d " " -f3'
net_recv_before=`pdsh -R ssh -w vm-23-[1-5] "$net_recv_command" | cut -d ' ' -f2 | python -c "import sys; print(sum(int(l) for l in sys.stdin))"`

#net_send_command='netstat -i | grep eth0 | tr -s " " | cut -d " " -f8'
net_send_command='cat /proc/net/dev | grep eth0 | tr -s " " | cut -d " " -f11'
net_send_before=`pdsh -R ssh -w vm-23-[1-5] "$io_write_command" | cut -d ' ' -f2 | python -c "import sys; print(sum(int(l) for l in sys.stdin))"`

start_spark_job

io_read_command='iostat -d | tail -2 | head -1 | tr -s " " | cut -d " " -f5'
io_read_after=`pdsh -R ssh -w vm-23-[1-5] "$io_read_command" | cut -d ' ' -f2 | python -c "import sys; print(sum(int(l) for l in sys.stdin))"`

io_write_command='iostat -d | tail -2 | head -1 | tr -s " " | cut -d " " -f6'
io_write_after=`pdsh -R ssh -w vm-23-[1-5] "$io_write_command" | cut -d ' ' -f2 | python -c "import sys; print(sum(int(l) for l in sys.stdin))"`

#net_recv_command='netstat -i | grep eth0 | tr -s " " | cut -d " " -f4'
net_recv_command='cat /proc/net/dev | grep eth0 | tr -s " " | cut -d " " -f3'
net_recv_after=`pdsh -R ssh -w vm-23-[1-5] "$net_recv_command" | cut -d ' ' -f2 | python -c "import sys; print(sum(int(l) for l in sys.stdin))"`

#net_send_command='netstat -i | grep eth0 | tr -s " " | cut -d " " -f8'
net_send_command='cat /proc/net/dev | grep eth0 | tr -s " " | cut -d " " -f11'
net_send_after=`pdsh -R ssh -w vm-23-[1-5] "$io_write_command" | cut -d ' ' -f2 | python -c "import sys; print(sum(int(l) for l in sys.stdin))"`

io_read=$(( io_read_after - io_read_before ))
io_write=$(( io_write_after - io_write_before ))

net_recv=$(( net_recv_after - net_recv_before ))
net_send=$(( net_send_after - net_send_before ))

echo "IO_read="$io_read" IO_write="$io_write
echo "Net_recv="$net_recv" Net_send="$net_send
