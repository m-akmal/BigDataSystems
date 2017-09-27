#!/bin/bash



echo "Clearing cache"
pdsh -R ssh -w vm-23-[1-5] 'sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"'
echo "Killing client"
kill $1


